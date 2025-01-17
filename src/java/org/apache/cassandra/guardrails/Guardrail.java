/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.guardrails;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;

import static java.lang.String.format;

/**
 * General class defining a given guardrail (that guards against some particular usage/condition).
 *
 * <p>Some guardrails only emit warnings when triggered, while other fail the query that trigger them. Some may do one
 * or the other based on specific threshold.
 *
 * <p>Note that all the defined class support live updates, which is why each guardrail class ctor takes suppliers of
 * the condition the guardrail acts on rather than the condition itself. Which does imply that said suppliers should
 * be fast and non-blocking to avoid surprises. Note that this does not mean live updates are exposed to the user,
 * just that the implementation is up to it if we ever want to expose it.
 */
public abstract class Guardrail
{
    private static final NoSpamLogger logger = NoSpamLogger.getLogger(LoggerFactory.getLogger(Guardrail.class),
                                                                      10, TimeUnit.MINUTES);
    private static final String REDACTED = "<redacted>";

    /** A name identifying the guardrail (mainly for shipping with Insights events). */
    public final String name;

    /** whether to throw {@link InvalidRequestException} on {@link this#fail(String)} */
    private boolean throwOnFailure = true;

    /** minimum logging and triggering interval to avoid spamming downstream*/
    private long minNotifyIntervalInMs = 0;

    /** time of last warning in milliseconds */
    private volatile long lastWarnInMs = 0;

    /** time of last failure in milliseconds */
    private volatile long lastFailInMs = 0;

    Guardrail(String name)
    {
        this.name = name;
    }

    protected void warn(String fullMessage, String redactedMessage)
    {
        if (skipNotifying(true))
            return;

        logger.warn(fullMessage);
        // Note that ClientWarn will simply ignore the message if we're not running this as part of a user query
        // (the internal "state" will be null)
        ClientWarn.instance.warn(fullMessage);
        // Similarly, tracing will also ignore the message if we're not running tracing on the current thread.
        Tracing.trace(fullMessage);
        for (Guardrails.Listener listener : Guardrails.listeners)
            listener.onWarningTriggered(name, redactedMessage);
    }

    protected void warn(String fullMessage)
    {
        warn(fullMessage, fullMessage);
    }

    protected void fail(String fullMessage, String redactedMessage)
    {
        if (!skipNotifying(false))
        {
            logger.error(fullMessage);
            // Tracing will ignore the message if we're not running tracing on the current thread.
            Tracing.trace(fullMessage);
            for (Guardrails.Listener listener : Guardrails.listeners)
                listener.onFailureTriggered(name, redactedMessage);
        }

        if (throwOnFailure)
            throw new InvalidRequestException(fullMessage);
    }

    protected void fail(String message)
    {
        fail(message, message);
    }

    /**
     * do no throw {@link InvalidRequestException} if guardrail failure is triggered.
     *
     * Note: this method is not thread safe and should only be used during guardrail initialization
     *
     * @return current guardrail
     */
    Guardrail noExceptionOnFailure()
    {
        this.throwOnFailure = false;
        return this;
    }

    /**
     * Note: this method is not thread safe and should only be used during guardrail initialization
     *
     * @param minNotifyIntervalInMs frequency of logging and triggering listener to avoid spamming,
     *                              default 0 means always log and trigger listeners.
     * @return current guardrail
     */
    Guardrail minNotifyIntervalInMs(long minNotifyIntervalInMs)
    {
        assert minNotifyIntervalInMs >= 0;
        this.minNotifyIntervalInMs = minNotifyIntervalInMs;
        return this;
    }

    /**
     * reset last notify time to make sure it will notify downstream when {@link this#warn(String, String)}
     * or {@link this#fail(String)} is called next time.
     */
    @VisibleForTesting
    public void resetLastNotifyTime()
    {
        lastFailInMs = 0;
        lastWarnInMs = 0;
    }

    /**
     * @return true if guardrail should not log message and trigger listeners; otherwise, update lastWarnInMs or
     * lastFailInMs respectively.
     */
    private boolean skipNotifying(boolean isWarn)
    {
        if (minNotifyIntervalInMs == 0)
            return false;

        long nowInMs = System.currentTimeMillis();
        long timeElapsedInMs = nowInMs - (isWarn ? lastWarnInMs : lastFailInMs);

        boolean skip = timeElapsedInMs < minNotifyIntervalInMs;

        if (!skip)
        {
            if (isWarn)
                lastWarnInMs = nowInMs;
            else
                lastFailInMs = nowInMs;
        }

        return skip;
    }

    /**
     * Checks whether this guardrail is enabled or not. This will be enabled if guardrails are globally enabled and
     * {@link Guardrails#ready()} and if the authenticated user (if specified) is not a system nor superuser.
     *
     * @param queryState the queryState, used to skip the check if the query is internal or is done by a superuser.
     *                   A {@code null} value means that the check should be done regardless of the query.
     * @return {@code true} if this guardrail is enabled & ready, {@code false} otherwise.
     */
    public boolean enabled(@Nullable QueryState queryState)
    {
        return Guardrails.ready() && (queryState == null || queryState.isOrdinaryUser());
    }

    /**
     * A guardrail based on numeric threshold(s).
     *
     * <p>A {@link Threshold} guardrail defines (up to) 2 threshold, one at which a warning is issued, and a higher one
     * at which a failure is triggered. Only one of those thresholds can be activated if desired.
     *
     * <p>This guardrail only handles guarding positive values.
     */
    public static class Threshold extends Guardrail
    {
        /**
         * A {@link Threshold} with both failure and warning thresholds disabled, so that cannot ever be triggered.
         */
        public static final Threshold NEVER_TRIGGERED = new Threshold("never_triggered", () -> -1L, () -> -1L, null);

        /**
         * A function used to build the error message of a triggered {@link Threshold} guardrail.
         */
        public interface ErrorMessageProvider
        {
            /**
             * Called when the guardrail is triggered to build the corresponding error message.
             *
             * @param isWarning       whether the trigger is a warning one; otherwise it is failure one.
             * @param what            a string, provided by the call to the {@link #guard} method, describing what the guardrail
             *                        has been applied to (and that has triggered it).
             * @param valueString     the value that triggered the guardrail (as a string).
             * @param thresholdString the threshold that was passed to trigger the guardrail (as a string).
             */
            String createMessage(boolean isWarning, String what, String valueString, String thresholdString);
        }

        final LongSupplier warnThreshold;
        final LongSupplier failThreshold;
        final ErrorMessageProvider errorMessageProvider;

        /**
         * Creates a new {@link Threshold} guardrail.
         *
         * @param name                 the name of the guardrail (for identification in {@link Guardrails.Listener} events).
         * @param warnThreshold        a supplier of the threshold above which a warning should be triggered. This cannot be
         *                             null, but {@code () -> -1L} can be provided if no warning threshold is desired.
         * @param failThreshold        a supplier of the threshold above which a failure should be triggered. This cannot be
         *                             null, but {@code () -> -1L} can be provided if no failure threshold is desired.
         * @param errorMessageProvider a function to generate the error message if the guardrail is triggered
         *                             (being it for a warning or a failure).
         */
        Threshold(String name,
                  LongSupplier warnThreshold,
                  LongSupplier failThreshold,
                  ErrorMessageProvider errorMessageProvider)
        {
            super(name);
            this.warnThreshold = warnThreshold;
            this.failThreshold = failThreshold;
            this.errorMessageProvider = errorMessageProvider;
        }

        protected String errMsg(boolean isWarning, String what, long value, long thresholdValue)
        {
            return errorMessageProvider.createMessage(isWarning,
                                                      what,
                                                      Long.toString(value),
                                                      Long.toString(thresholdValue));
        }

        protected String redactedErrMsg(boolean isWarning, long value, long thresholdValue)
        {
            return errorMessageProvider.createMessage(isWarning,
                                                      REDACTED,
                                                      Long.toString(value),
                                                      Long.toString(thresholdValue));
        }

        private long failValue()
        {
            long failValue = failThreshold.getAsLong();
            return failValue < 0 ? Long.MAX_VALUE : failValue;
        }

        private long warnValue()
        {
            long warnValue = warnThreshold.getAsLong();
            return warnValue < 0 ? Long.MAX_VALUE : warnValue;
        }

        /**
         * Checks whether this guardrail is enabled or not. This will be enabled if guardrails are
         * ({@link Guardrails#ready()} ()}), the keyspace (if specified) is not an internal one, and if any of the
         * thresholds is positive.
         *
         * @param queryState the queryState, used to skip the check if the query is internal or is done by a superuser.
         *                   A {@code null} value means that the check should be done regardless of the query.
         * @return {@code true} if this guardrail is enabled, {@code false} otherwise.
         */
        public boolean enabled(@Nullable QueryState queryState)
        {
            return super.enabled(queryState) && (failThreshold.getAsLong() >= 0 || warnThreshold.getAsLong() >= 0);
        }

        /**
         * Checks whether the provided value would trigger a warning or failure if passed to {@link #guard}.
         *
         * <p>This method is optional (does not have to be called) but can be used in the case where the "what"
         * argument to {@link #guard} is expensive to build to save doing so in the common case (of the guardrail
         * not being triggered).
         *
         * @param value      the value to test.
         * @param queryState the queryState, used to skip the check if the query is internal or is done by a superuser.
         *                   A {@code null} value means that the check should be done regardless of the query.
         * @return {@code true} if {@code value} is above the warning or failure thresholds of this guardrail,
         * {@code false otherwise}.
         */
        public boolean triggersOn(long value, @Nullable QueryState queryState)
        {
            return enabled(queryState) && (value > Math.min(failValue(), warnValue()));
        }

        /**
         * Apply the guardrail to the provided value, triggering a warning or failure if appropriate.
         *
         * @param value            the value to check.
         * @param what             a string describing what {@code value} is a value of used in the error message if the
         *                         guardrail is triggered (for instance, say the guardrail guards the size of column values, then this
         *                         argument must describe which column of which row is triggering the guardrail for convenience). Note that
         *                         this is only used if the guardrail triggers, so if it is expensive to build, you can put the call to
         *                         this method behind a {@link #triggersOn} call.
         * @param containsUserData a boolean describing if {@code what} contains user data. If this is the case,
         *                         {@code what} will only be included in the log messages and client warning. It will not be included in the
         *                         error messages that are passed to listeners and exceptions. We have to exclude the user data from
         *                         exceptions because they are sent to Insights.
         * @param queryState       the query state, used to skip the check if the query is internal or is done by a superuser.
         *                         A {@code null} value means that the check should be done regardless of the query.
         */
        public void guard(long value, String what, boolean containsUserData, @Nullable QueryState queryState)
        {
            if (!enabled(queryState))
                return;

            long failValue = failValue();
            if (value > failValue)
            {
                triggerFail(value, failValue, what, containsUserData);
                return;
            }

            long warnValue = warnValue();
            if (value > warnValue)
                triggerWarn(value, warnValue, what, containsUserData);
        }

        private void triggerFail(long value, long failValue, String what, boolean containsUserData)
        {
            String fullMsg = errMsg(false, what, value, failValue);
            fail(fullMsg, containsUserData ? redactedErrMsg(false, value, failValue) : fullMsg);
        }

        private void triggerWarn(long value, long warnValue, String what, boolean containsUserData)
        {
            String fullMsg = errMsg(true, what, value, warnValue);
            warn(fullMsg, containsUserData ? redactedErrMsg(true, value, warnValue) : fullMsg);
        }

        /**
         * Creates a new {@link GuardedCounter} guarded by this threshold guardrail.
         *
         * @param whatFct          a function called when either a warning or failure is triggered by the created counter to
         *                         describe the value. This is equivalent to the {@code what} argument of {@link #guard} but is a function to
         *                         allow the output string to be compute lazily (only if a failure/warn ends up being triggered).
         * @param containsUserData if a warning or failure is triggered by the created counter and the {@code whatFct}
         *                         is called, indicates whether the create string contains user data. This is the exact equivalent to the
         *                         similarly named argument of {@link #guard}.
         * @param queryState       the query state, used to skip the check if the query is internal or is done by a superuser.
         *                         A {@code null} value means that the check should be done regardless of the query.
         * @return the newly created guarded counter.
         */
        public GuardedCounter newCounter(Supplier<String> whatFct, boolean containsUserData, @Nullable QueryState queryState)
        {
            Threshold threshold = enabled(queryState) ? this : NEVER_TRIGGERED;
            return threshold.new GuardedCounter(whatFct, containsUserData);
        }

        /**
         * A facility for when the value to guard is built incrementally, but we want to trigger failures as soon
         * as the failure threshold is reached, but only trigger the warning on the final value (and so only if the
         * failure threshold hasn't also been reached).
         * <p>
         * Note that instances are neither thread safe nor reusable.
         */
        public class GuardedCounter
        {
            private final long warnValue;
            private final long failValue;
            private final Supplier<String> what;
            private final boolean containsUserData;

            private long accumulated;

            private GuardedCounter(Supplier<String> what, boolean containsUserData)
            {
                // We capture the warn and fail value at the time of the counter construction to ensure we use
                // stable value during the counter lifetime (and reading a final field is possibly at tad faster).
                this.warnValue = warnValue();
                this.failValue = failValue();
                this.what = what;
                this.containsUserData = containsUserData;
            }

            /**
             * The currently accumulated value of the counter.
             */
            public long get()
            {
                return accumulated;
            }

            /**
             * Add the provided increment to the counter, triggering a failure if the counter after this addition
             * crosses the failure threshold.
             *
             * @param increment the increment to add.
             */
            public void add(long increment)
            {
                accumulated += increment;
                if (accumulated > failValue)
                    triggerFail(accumulated, failValue, what.get(), containsUserData);
            }

            /**
             * Trigger the warn if the currently accumulated counter value crosses warning threshold and the failure
             * has not been triggered yet.
             * <p>
             * This is generally meant to be called when the guarded value is complete.
             *
             * @return {@code true} and trigger a warning if the current counter value is greater than the warning
             * threshold and less than or equal to the failure threshold, {@code false} otherwise.
             */
            public boolean checkAndTriggerWarning()
            {
                if (accumulated > warnValue && accumulated <= failValue)
                {
                    triggerWarn(accumulated, warnValue, what.get(), containsUserData);
                    return true;
                }
                return false;
            }
        }
    }

    /**
     * A {@link Threshold} guardrail whose values represent a byte size.
     *
     * <p>This works exactly as a {@link Threshold}, but provides slightly more convenient error messages (display
     * the sizes in human readable format).
     */
    public static class SizeThreshold extends Threshold
    {
        SizeThreshold(String name,
                      LongSupplier warnThreshold,
                      LongSupplier failThreshold,
                      ErrorMessageProvider errorMessageProvider)
        {
            super(name, warnThreshold, failThreshold, errorMessageProvider);
        }

        @Override
        protected String errMsg(boolean isWarning, String what, long value, long thresholdValue)
        {
            return errorMessageProvider.createMessage(isWarning,
                                                      what,
                                                      Units.toString(value, SizeUnit.BYTES),
                                                      Units.toString(thresholdValue, SizeUnit.BYTES));
        }

        @Override
        protected String redactedErrMsg(boolean isWarning, long value, long thresholdValue)
        {
            return errorMessageProvider.createMessage(isWarning,
                                                      REDACTED,
                                                      Units.toString(value, SizeUnit.BYTES),
                                                      Units.toString(thresholdValue, SizeUnit.BYTES));
        }
    }

    /**
     * A {@link Threshold} guardrail whose values represent a percentage
     *
     * <p>This work exactly as a {@link Threshold}, but provides slightly more convenient error messages for percentage
     */
    public static class PercentageThreshold extends Threshold
    {
        PercentageThreshold(String name,
                            LongSupplier warnThreshold,
                            LongSupplier failThreshold,
                            ErrorMessageProvider errorMessageProvider)
        {
            super(name, warnThreshold, failThreshold, errorMessageProvider);
        }

        @Override
        protected String errMsg(boolean isWarning, String what, long value, long thresholdValue)
        {
            return errorMessageProvider.createMessage(isWarning,
                                                      what,
                                                      String.format("%d%%", value),
                                                      String.format("%d%%", thresholdValue));
        }
    }

    /**
     * A guardrail that completely disables the use of a particular feature.
     *
     * <p>Note that this guardrail only triggers failures (if the feature is disabled) so is only meant for
     * query-based guardrails (we're happy to reject queries deemed dangerous, but we don't want to create a guardrail
     * that breaks compaction for instance).
     */
    public static class DisableFlag extends Guardrail
    {
        private final BooleanSupplier disabled;
        private final String what;

        /**
         * Creates a new {@link DisableFlag} guardrail.
         *
         * @param name     the name of the guardrail (for identification in {@link Guardrails.Listener} events).
         * @param disabled a supplier of boolean indicating whether the feature guarded by this guardrail must be
         *                 disabled.
         * @param what     the feature that is guarded by this guardrail (for reporting in error messages),
         *                 {@link #ensureEnabled(String, QueryState)}}} can specify a different {@code what}.
         */
        DisableFlag(String name, BooleanSupplier disabled, String what)
        {
            super(name);
            this.disabled = disabled;
            this.what = what;
        }

        /**
         * Triggers a failure if this guardrail is disabled.
         *
         * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
         * allowed.
         *
         * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
         * allowed.
         *
         * @param queryState the queryState, used to skip the check if the query is internal or is done by a superuser.
         *                   A {@code null} value means that the check should be done regardless of the query.
         */
        public void ensureEnabled(@Nullable QueryState queryState)
        {
            ensureEnabled(what, queryState);
        }

        /**
         * Triggers a failure if this guardrail is disabled.
         *
         * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
         * allowed.
         *
         * @param what       the feature that is guarded by this guardrail (for reporting in error messages).
         * @param queryState the queryState, used to skip the check if the query is internal or is done by a superuser.
         *                   A {@code null} value means that the check should be done regardless of the query.
         */
        public void ensureEnabled(String what, @Nullable QueryState queryState)
        {
            if (enabled(queryState) && disabled.getAsBoolean())
                fail(what + " is not allowed");
        }
    }

    /**
     * Base class for guardrail that are triggered based on a set of values.
     *
     * @param <T> the type of the values that trigger the guardrail.
     */
    private static abstract class ValuesBaseGuardrail<T> extends Guardrail
    {
        /*
         * Implementation note: as mentioned in the class Javadoc and for consistency with the other Guardrail
         * implementation of this class (and to generally avoid surprises), this implementation ensures that live
         * changes to the underlying guardrail setting gets reflected. This is the reason for the relative
         * "complexity" of this class.
         */

        private final Supplier<Set<String>> rawSupplier;
        private final Function<String, T> parser;
        protected final String what;

        private volatile ImmutableSet<T> cachedValues;
        private volatile Set<String> cachedRaw;

        protected ValuesBaseGuardrail(
            String name, Supplier<Set<String>> disallowedRaw, Function<String, T> parser, String what)
        {
            super(name);
            this.rawSupplier = disallowedRaw;
            this.parser = parser;
            this.what = what;

            if (Guardrails.ready())
                ensureUpToDate();
        }

        protected void ensureUpToDate()
        {
            Set<String> current = rawSupplier.get();
            // Same as below, this shouldn't happen if settings have been properly sanitized, but throw a meaningful
            // error if there is a bug.
            if (current == null)
                throw new RuntimeException(format("Invalid null setting for guardrail on %s. This should not have"
                                                  + " happened", what));

            // Note that this will fail on first call (as we want), as currentRaw will be null but not current
            if (current == cachedRaw)
                return;

            try
            {
                // Setting cachedAllowed first so that on a parse failure we leave everything as it previously
                // was (not that we'd expect that matter but ...).
                cachedValues = current.stream()
                                      .map(parser)
                                      .collect(ImmutableSet.toImmutableSet());
                cachedRaw = current;
            }
            catch (Exception e)
            {
                // This catches parsing errors. Hopefully, this shouldn't happen as guardrails settings should have
                // been sanitized, but ...
                // Also, we catch the exception to add a meaningful error message, but rethrow otherwise: if a
                // guardrail has been configured, it's presumably to avoid bad things to go in, so we don't want to
                // take the risk of letting it go if there is a misconfiguration.
                throw new RuntimeException(format("Error parsing configured setting for guardrail on %s. This "
                                                  + "is a bug and should not have happened."
                                                  + "The failing setting is %s", what, current), e);
            }
        }

        protected Set<T> matchingValues(Set<T> values) {
            return Sets.intersection(values, cachedValues);
        }

        protected String triggerValuesString()
        {
            return cachedRaw.toString();
        }

        /**
         * Checks whether the provided value would trigger this guardrail.
         *
         * <p>This method is optional (does not have to be called) but can be used in the case some of the arguments
         * to the actual guardrail method is expensive to build to save doing so in the common case (of the
         * guardrail not being triggered).
         *
         * @param value the value to test.
         * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
         * @return {@code true} if {@code value} is not allowed by this guardrail,
         * {@code false otherwise}.
         */
        public boolean triggersOn(T value, @Nullable QueryState state)
        {
            if (!enabled(state))
                return false;

            ensureUpToDate();
            return cachedValues.contains(value);
        }
    }


    /**
     * A guardrail that rejects the use of specific values.
     *
     * <p>Note that like {@link DisableFlag}, this guardrail only trigger failures and is thus only for query-based
     * guardrails.
     *
     * @param <T> the type of the values of which certain are disallowed.
     */
    public static class DisallowedValues<T> extends ValuesBaseGuardrail<T>
    {
        /**
         * Creates a new {@link DisallowedValues} guardrail.
         *
         * @param name the name of the guardrail (for identification in {@link Guardrails.Listener} events).
         * @param disallowedRaw a supplier of the values that are disallowed in raw (string) form. The set returned by
         *                      this supplier <b>must</b> be immutable (we don't use {@code ImmutableSet} because we
         *                      want to feed values from {@link GuardrailsConfig} directly and having ImmutableSet
         *                      there would currently be annoying (because populated automatically by snakeYaml)).
         * @param parser a function to parse the value to disallow from string.
         * @param what what represents the value disallowed (for reporting in error messages).
         */
        DisallowedValues(String name, Supplier<Set<String>> disallowedRaw, Function<String, T> parser, String what)
        {
            super(name, disallowedRaw, parser, what);
        }

        /**
         * Triggers a failure if the provided value is disallowed by this guardrail.
         *
         * @param value the value to check.
         * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
         * A {@code null} value means that the check should be done regardless of the query.
         */
        public void ensureAllowed(T value, @Nullable QueryState state)
        {
            if (triggersOn(value, state))
                fail(format("Provided value %s is not allowed for %s (disallowed values are: %s)",
                            value, what, triggerValuesString()));
        }

        /**
         * Triggers a failure if any of the provided values is disallowed by this guardrail.
         *
         * @param values the values to check.
         * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
         * A {@code null} value means that the check should be done regardless of the query.
         */
        public void ensureAllowed(Set<T> values, @Nullable QueryState state)
        {
            if (!enabled(state))
                return;

            ensureUpToDate();

            Set<T> disallowed = matchingValues(values);
            if (!disallowed.isEmpty())
                fail(format("Provided values %s are not allowed for %s (disallowed values are: %s)",
                            disallowed.stream().sorted().collect(Collectors.toList()), what, triggerValuesString()));
        }
    }

    /**
     * A guardrail based on two predicates.
     *
     * <p>A {@link Predicates} guardrail defines (up to) 2 predicates, one at which a warning is issued, and another one
     * at which a failure is triggered. If failure is triggered, warning is skipped.
     *
     * @param <T> the type of the values to be tested against predicates.
     */
    public static class Predicates<T> extends Guardrail
    {
        private final Predicate<T> warnPredicate;
        private final Predicate<T> failurePredicate;
        private final MessageProvider<T> messageProvider;

        /**
         * A function used to build the warning or error message of a triggered {@link Predicates} guardrail.
         */
        public interface MessageProvider<T>
        {
            /**
             * Called when the guardrail is triggered to build the corresponding message.
             *
             * @param isWarning whether the trigger is a warning one; otherwise it is failure one.
             * @param value     the value that triggers guardrail.
             */
            String createMessage(boolean isWarning, T value);
        }

        /**
         * Creates a new {@link Predicates} guardrail.
         *
         * @param name             the name of the guardrail (for identification in {@link Guardrails.Listener} events).
         * @param warnPredicate    a predicate that is used to check if given value should trigger a warning.
         * @param failurePredicate a predicate that is used to check if given value should trigger a failure.
         * @param messageProvider  a function to generate the warning or error message if the guardrail is triggered
         */
        Predicates(String name, Predicate<T> warnPredicate, Predicate<T> failurePredicate, MessageProvider<T> messageProvider)
        {
            super(name);
            this.warnPredicate = warnPredicate;
            this.failurePredicate = failurePredicate;
            this.messageProvider = messageProvider;
        }

        /**
         * Apply the guardrail to the provided value, triggering a warning or failure if appropriate.
         *
         * @param value      the value to check.
         * @param queryState the query queryState, used to skip the check if the query is internal or is done by a superuser.
         *                   A {@code null} value means that the check should be done regardless of the query.
         */
        public void guard(T value, @Nullable QueryState queryState)
        {
            if (!enabled(queryState))
                return;

            if (failurePredicate.test(value))
            {
                fail(messageProvider.createMessage(false, value));
            }
            else if (warnPredicate.test(value))
            {
                warn(messageProvider.createMessage(true, value));
            }
        }
    }
    
    /**
     * A guardrail that warns but ignore some specific values.
     *
     * @param <T> the type of the values of which certain are ignored.
     */
    public static class IgnoredValues<T> extends ValuesBaseGuardrail<T>
    {
        /**
         * Creates a new {@link IgnoredValues} guardrail.
         *
         * @param name the name of the guardrail (for identification in {@link Guardrails.Listener} events).
         * @param ignoredRaw a supplier of the values that are ignored in raw (string) form. The set returned by
         *                      this supplier <b>must</b> be immutable (we don't use {@code ImmutableSet} because we
         *                      want to feed values from {@link GuardrailsConfig} directly and having ImmutableSet
         *                      there would currently be annoying (because populated automatically by snakeYaml)).
         * @param parser a function to parse the value to ignore from string.
         * @param what what represents the value ignored (for reporting in error messages).
         */
        IgnoredValues(String name, Supplier<Set<String>> ignoredRaw, Function<String, T> parser, String what)
        {
            super(name, ignoredRaw, parser, what);
        }

        /**
         * Checks for ignored values by this guardrail and when it found some, log a warning and trigger an action
         * to ignore them.
         *
         * @param values the values to check.
         * @param ignoreAction an action called on the subset of {@code values} that should be ignored. This action
         * should do whatever is necessary to make sure the value is ignored.
         * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
         * A {@code null} value means that the check should be done regardless of the query.
         */
        public void maybeIgnoreAndWarn(Set<T> values, Consumer<T> ignoreAction, @Nullable QueryState state)
        {
            if (!enabled(state))
                return;

            ensureUpToDate();

            Set<T> toIgnore = matchingValues(values);
            if (toIgnore.isEmpty())
                return;

            warn(format("Ignoring provided values %s as they are not supported for %s (ignored values are: %s)",
                        toIgnore.stream().sorted().collect(Collectors.toList()), what, triggerValuesString()));
            for (T value : toIgnore)
                ignoreAction.accept(value);
        }
    }
}

