import type {
  RuntimeTelemetryEvent,
  RuntimeTelemetryFlusher,
  RuntimeTelemetrySink
} from "@ramideltoro/nutsnews-worker-runtime";

export function createBestEffortRuntimeTelemetrySink(
  sink: RuntimeTelemetrySink | undefined
): RuntimeTelemetrySink {
  return {
    emit: (event) => emitWithoutAffectingRuntime(sink, event)
  };
}

export function combineBestEffortRuntimeTelemetrySinks(
  ...sinks: readonly (RuntimeTelemetrySink | undefined)[]
): RuntimeTelemetrySink | undefined {
  const configured = sinks.filter((sink): sink is RuntimeTelemetrySink => sink !== undefined);

  if (configured.length === 0) {
    return undefined;
  }

  return {
    emit: async (event) => {
      for (const sink of configured) {
        await emitWithoutAffectingRuntime(sink, event);
      }
    }
  };
}

export function createBestEffortRuntimeTelemetryFlusher(
  flusher: RuntimeTelemetryFlusher
): RuntimeTelemetryFlusher {
  return {
    flush: async () => {
      try {
        await flusher.flush();
      } catch {
        // Flush failures cannot turn a completed graceful shutdown into a failure.
      }
    }
  };
}

async function emitWithoutAffectingRuntime(
  sink: RuntimeTelemetrySink | undefined,
  event: RuntimeTelemetryEvent
): Promise<void> {
  try {
    await sink?.emit(event);
  } catch {
    // A failed telemetry destination must never change business processing.
  }
}
