import * as pathLib from "path";
import * as FirebaseError from "../error";
import * as logger from "../logger";

import { NodeSize, RemoveRemote, RTDBRemoveRemote } from "./removeRemote";
import { Stack } from "../throttler/stack";

export interface DatabaseRemoveOptions {
  // RTBD instance ID.
  instance: string;
  // Number of concurrent chunk deletes to allow.
  concurrency: number;
  // Number of retries for each chunk delete.
  retries: number;
}

export default class DatabaseRemove {
  public path: string;
  public concurrency: number;
  public retries: number;
  public remote: RemoveRemote;
 //  private jobStack: Stack;

  /**
   * Construct a new RTDB delete operation.
   *
   * @constructor
   * @param path path to delete.
   * @param options
   */
  constructor(path: string, options: DatabaseRemoveOptions) {
    this.path = path;
    this.concurrency = options.concurrency;
    this.retries = options.retries;
    this.remote = new RTDBRemoveRemote(options.instance);
    // this.jobStack = new Stack({
    //   name: "long delete stack",
    //   concurrency: this.concurrency,
    //   handler: this.chunkedDelete.bind(this),
    //   retries: this.retries,
    //  });
  }

  public execute(): Promise<void> {
    return this.chunkedDelete(this.path);
  }

  private async chunkedDelete(path: string): Promise<void> {
    switch (await this.remote.prefetchTest(path)) {
      case NodeSize.SMALL:
        await this.remote.deletePath(path);
        break;
      case NodeSize.LARGE:
        const pathList = await this.remote.listPath(path);
        await Promise.all(pathList.map((p) => this.chunkedDelete(pathLib.join(path, p))));
        await this.chunkedDelete(path);
        break;
      case NodeSize.EMPTY:
        break;
      default:
        throw new FirebaseError("Unexpected prefetch test result: " + test, { exit: 3 });
    }
  }
}
