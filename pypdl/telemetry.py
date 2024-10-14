from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from .config import Config

resource = Resource(attributes={SERVICE_NAME: "async-file-downloader"})

trace_provider = TracerProvider(resource=resource)
otlp_span_exporter = OTLPSpanExporter(endpoint=Config.OTLP_ENDPOINT)
span_processor = BatchSpanProcessor(otlp_span_exporter)
trace_provider.add_span_processor(span_processor)
trace.set_tracer_provider(trace_provider)

metric_reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=Config.OTLP_ENDPOINT))
metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(metric_provider)

tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)

download_counter = meter.create_counter(
    name="file_downloads",
    description="The number of files downloaded",
    unit="1"
)

download_size_histogram = meter.create_histogram(
    name="download_size",
    description="The size of downloaded files",
    unit="bytes"
)

download_duration_histogram = meter.create_histogram(
    name="download_duration",
    description="The duration of file downloads",
    unit="seconds"
)
