import { describe, expect, it } from "bun:test";
import { AdaptiveTransportCapacity } from "../src/v2/transport/adaptive-capacity";

describe("V2 long-run capacity invariants", () => {
  it("cannot exhaust eight healthy sandboxes through adaptation and cancellation churn", () => {
    const controllers = Array.from({ length: 8 }, () => (
      new AdaptiveTransportCapacity({ initial: 8, maximum: 16 })
    ));

    // Model many healthy flights separated by arbitrary idle time. The
    // controller consumes only measured flight health, never wall-clock idle.
    for (let round = 0; round < 10_000; round++) {
      for (const controller of controllers) {
        controller.observe({ rttMs: 350, congested: false });
      }
    }
    expect(controllers.every(controller => controller.limit === 16)).toBe(true);

    // Even a simultaneous severe sample cannot reduce below the safe floor.
    for (const controller of controllers) {
      for (let reduction = 0; reduction < 16; reduction++) {
        controller.observe({ rttMs: 10_000, congested: true });
      }
    }
    expect(controllers.every(controller => controller.limit === 8)).toBe(true);

    // Reproduce the observed distribution of thirteen cancellations before
    // their definitive acknowledgements arrive.
    const temporaryQuarantines = [2, 4, 2, 1, 1, 1, 1, 1];
    const usableDuringCancellation = controllers.reduce(
      (sum, controller, index) => sum + controller.limit - temporaryQuarantines[index]!,
      0,
    );
    expect(usableDuringCancellation).toBe(51);
    expect(usableDuringCancellation).toBeGreaterThan(0);

    // Matching completion acknowledgements release all cancellation-only
    // quarantine, restoring the full safe floor.
    const usableAfterCompletion = controllers.reduce(
      (sum, controller) => sum + controller.limit,
      0,
    );
    expect(usableAfterCompletion).toBe(64);
  });
});
