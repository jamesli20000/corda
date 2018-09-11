package net.corda.nodeapi.internal.tracing

import co.paralleluniverse.strands.Strand
import io.jaegertracing.Configuration
import io.jaegertracing.Configuration.*
import io.jaegertracing.internal.samplers.ConstSampler
import io.opentracing.Span
import io.opentracing.Tracer
import io.opentracing.log.Fields
import io.opentracing.tag.Tags
import io.opentracing.util.GlobalTracer
import net.corda.core.internal.FlowStateMachine
import java.util.concurrent.ConcurrentHashMap

class CordaTracer private constructor(private val tracer: Tracer) {

    class Builder {
        var endpoint: String = "http://localhost:14268/api/traces"
            private set

        var serviceName: String = "Corda"
            private set

        var flushInterval: Int = 200
            private set

        var logSpans: Boolean = true
            private set

        fun withEndpoint(endpoint: String) {
            this.endpoint = endpoint
        }

        fun withServiceName(serviceName: String) {
            this.serviceName = serviceName
        }

        fun withFlushInterval(interval: Int) {
            this.flushInterval = interval
        }

        fun withLogSpans(logSpans: Boolean) {
            this.logSpans = logSpans
        }
    }

    companion object {

        private val tracer: Tracer by lazy {
            val builder = Builder()
            val sampler = SamplerConfiguration.fromEnv()
                    .withType(ConstSampler.TYPE)
                    .withParam(1)
            val sender = SenderConfiguration.fromEnv()
                    .withEndpoint(builder.endpoint)
            val reporter = ReporterConfiguration.fromEnv()
                    .withSender(sender)
                    .withLogSpans(builder.logSpans)
                    .withFlushInterval(builder.flushInterval)
            val tracer: Tracer = Configuration(builder.serviceName)
                    .withSampler(sampler)
                    .withReporter(reporter)
                    .tracer
            GlobalTracer.register(tracer)
            tracer
        }

        val current = CordaTracer(tracer)

        fun Span?.tag(key: String, value: Any?) {
            this?.setTag(key, value?.toString())
        }

        fun Span?.error(message: String, exception: Throwable? = null) {
            val span = this ?: return
            Tags.ERROR.set(span, true)
            if (exception != null) {
                span.log(mapOf(
                        Fields.EVENT to "error",
                        Fields.ERROR_OBJECT to exception,
                        Fields.MESSAGE to message
                ))
            } else {
                span.log(mapOf(
                        Fields.EVENT to "error",
                        Fields.MESSAGE to message
                ))
            }
        }

    }

    fun terminate() {
        rootSpan?.finish()
    }

    var rootSpan: Span? = null

    private var flowSpans: ConcurrentHashMap<String, Span> = ConcurrentHashMap()

    private val flow: FlowStateMachine<*>?
        get() = Strand.currentStrand() as? FlowStateMachine<*>

    private val FlowStateMachine<*>.flowId: String
        get() = this.id.uuid.toString()

    private fun Tracer.SpanBuilder.decorate() {
        flow?.apply {
            withTag("flow-id", flowId)
            withTag("fiber-id", Strand.currentStrand().id.toString())
            withTag("thread-id", Thread.currentThread().id.toString())
        }
    }

    private fun createRootSpan(): Span? {
        rootSpan = tracer.buildSpan("Execution").start()
        return rootSpan
    }

    fun <T> flowSpan(action: (Span, FlowStateMachine<*>) -> T): T? {
        return flow?.let { flow ->
            val span = flowSpans.getOrPut(flow.flowId) {
                tracer.buildSpan(flow.logic.toString()).apply {
                    (rootSpan ?: createRootSpan())?.apply { asChildOf(this) }
                    decorate()

                }.start()
            }
            action(span, flow)
        }
    }

    fun span(name: String): Span? {
        return flowSpan { parentSpan, flow ->
            tracer.buildSpan(name).apply {
                asChildOf(parentSpan)
                withTag("flow-id", flow.id.uuid.toString())
                withTag("fiber-id", Strand.currentStrand().id.toString())
                withTag("thread-id", Thread.currentThread().id.toString())

            }.start()
        }
    }

    fun <T> span(name: String, block: (Span?) -> T): T {
        return span(name)?.let { span ->
            try {
                block(span)
            } catch (ex: Exception) {
                Tags.ERROR.set(span, true)
                span.log(mapOf(
                        Fields.EVENT to "error",
                        Fields.ERROR_OBJECT to ex,
                        Fields.MESSAGE to ex.message
                ))
                throw ex
            } finally {
                span.finish()
            }
        } ?: block(null)
    }

    fun endFlow() {
        flow?.id?.uuid.toString().let { flowId ->
            val span = flowSpans.remove(flowId)
            span?.finish()
        }
    }

}