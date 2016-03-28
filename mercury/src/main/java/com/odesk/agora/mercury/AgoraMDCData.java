package com.odesk.agora.mercury;

/**
 * Created by Dmitry Solovyov on 03/16/2016.
 * <p>
 * A simple container for agora MDC data: requestId, traceId, spanId, parentSpanId.
 */
public class AgoraMDCData {
    private final String requestId;
    private final String traceId;
    private final String spanId;
    private final String parentSpanId;

    public AgoraMDCData() {
        this(null, null, null, null);
    }

    public AgoraMDCData(String requestId, String traceId, String spanId, String parentSpanId) {
        this.requestId = requestId;
        this.traceId = traceId;
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getTraceId() {
        return traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    public String getParentSpanId() {
        return parentSpanId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("requestId=" + requestId + ", ");
        sb.append("traceId=" + traceId + ", ");
        sb.append("spanId=" + spanId + ", ");
        sb.append("parentSpanId=" + parentSpanId);
        sb.append("}");
        return sb.toString();
    }
}
