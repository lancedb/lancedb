export type WorkerRequest =
  | {
      id: number;
      type: "open";
      tableUrl: string;
      optionsJson?: string;
    }
  | {
      id: number;
      type: "schema";
    }
  | {
      id: number;
      type: "search";
      requestJson: string;
    }
  | {
      id: number;
      type: "refresh";
    };

export type WorkerResponse =
  | {
      id: number;
      ok: true;
      type: "open";
    }
  | {
      id: number;
      ok: true;
      type: "schema";
      bytes: ArrayBuffer;
    }
  | {
      id: number;
      ok: true;
      type: "search";
      bytes: ArrayBuffer;
    }
  | {
      id: number;
      ok: true;
      type: "refresh";
      changed: boolean;
    }
  | {
      id: number;
      ok: false;
      error: string;
    };

export type WorkerSuccessResponse = Extract<WorkerResponse, { ok: true }>;
