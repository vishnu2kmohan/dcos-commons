package com.mesosphere.sdk.offer.evaluate;

import java.util.Collection;
import java.util.Collections;

/**
 * The outcome of invoking an {@link OfferEvaluationStage}. Describes whether the evaluation passed or failed, and the
 * reason(s) why. Supports a nested tree of outcomes which describe any sub-evaluations which may have been performed
 * within the {@link OfferEvaluationStage}.
 */
public class EvaluationOutcome {

    /**
     * The outcome value.
     */
    private enum Type {
        PASS,
        FAIL
    }

    private final Type type;
    private final String source;
    private final Collection<EvaluationOutcome> children;
    private final String reason;

    /**
     * Returns a new passing outcome object with the provided descriptive reason.
     *
     * @param source the object which produced this outcome, whose class name will be labeled as the origin
     * @param reasonFormat {@link String#format(String, Object...)} compatible format string describing the pass reason
     * @param reasonArgs format arguments, if any, to apply against {@code reasonFormat}
     */
    public static EvaluationOutcome pass(Object source, String reasonFormat, Object... reasonArgs) {
        return create(true, source, Collections.emptyList(), reasonFormat, reasonArgs);
    }

    /**
     * Returns a new failing outcome object with the provided descriptive reason.
     *
     * @param source the object which produced this outcome, whose class name will be labeled as the origin
     * @param reasonFormat {@link String#format(String, Object...)} compatible format string describing the fail reason
     * @param reasonArgs format arguments, if any, to apply against {@code reasonFormat}
     */
    public static EvaluationOutcome fail(Object source, String reasonFormat, Object... reasonArgs) {
        return create(false, source, Collections.emptyList(), reasonFormat, reasonArgs);
    }

    /**
     * Returns a new outcome object with the provided outcome type, descriptive reason, and child outcomes.
     */
    public static EvaluationOutcome create(
            boolean isPassing,
            Object source,
            Collection<EvaluationOutcome> children,
            String reasonFormat,
            Object... reasonArgs) {
        return new EvaluationOutcome(isPassing ? Type.PASS : Type.FAIL, source, children, reasonFormat, reasonArgs);
    }

    private EvaluationOutcome(
            Type type,
            Object source,
            Collection<EvaluationOutcome> children,
            String reasonFormat,
            Object... reasonArgs) {
        this.type = type;
        this.source = source.getClass().getSimpleName();
        this.children = children;
        this.reason = String.format(reasonFormat, reasonArgs);
    }

    /**
     * Returns whether this outcome was passing ({@code true}) or failing ({@code false}).
     */
    public boolean isPassing() {
        return type == Type.PASS;
    }

    /**
     * Returns the name of the object which produced this response.
     */
    public String getSource() {
        return source;
    }

    /**
     * Returns the reason that this response is passing or failing.
     */
    public String getReason() {
        return reason;
    }

    /**
     * Returns any nested outcomes which resulted in this decision.
     */
    public Collection<EvaluationOutcome> getChildren() {
        return children;
    }

    @Override
    public String toString() {
        return String.format("%s(%s): %s", isPassing() ? "PASS" : "FAIL", getSource(), getReason());
    }
}