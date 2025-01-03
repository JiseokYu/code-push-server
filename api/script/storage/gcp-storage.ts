// Copyright Jiseok Yu.
// Licensed under the MIT License.

import * as q from "q";
import * as stream from "stream";
import * as storage from "./storage";

import { Bucket, Storage as GCPRawStorage } from "@google-cloud/storage";
import { Firestore } from "@google-cloud/firestore";
import { v4 as uuidv4 } from "uuid";
import { DeploymentInfo } from "./storage";

interface AppPointer {
  appId: string;
  accountId: string;
}

export class GCPStorage implements storage.Storage {
  private static MAX_PACKAGE_HISTORY_LENGTH = 50;

  private _firestore: Firestore;
  private _storage_bucket: Bucket;
  private _setupPromise: q.Promise<void>;

  constructor(projectId?: string) {
    this._setupPromise = this.setup(projectId);
  }

  public checkHealth(): q.Promise<void> {
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("health").doc("health").get()
      })
      .then((doc) => {
        if (!doc.exists) {
          throw storage.storageError(storage.ErrorCode.ConnectionFailed, "The GCP Firestore service failed the health check");
        }
        return this.blobHealthCheck();
      });
  }

  private blobHealthCheck(): q.Promise<void> {
    const file = this._storage_bucket.file("health");
    return q.Promise<void>((resolve, reject) => {
      file
        .download()
        .then(([content]) => {
          if (content.toString() !== "health") {
            reject(storage.storageError(storage.ErrorCode.ConnectionFailed, "The GCP Storage service failed the health check"));
          } else {
            resolve();
          }
        })
        .catch(reject);
    });
  }

  private setup(projectId?: string): q.Promise<void> {
    const options: any = {};

    if (process.env.EMULATED === "true") {
      options.projectId = "test-project";
      options.apiEndpoint = "localhost:8081";
    } else {
      if (!projectId && !process.env.GOOGLE_CLOUD_PROJECT) {
          throw new Error("GCP credentials not set");
      }
      options.projectId = projectId || process.env.GOOGLE_CLOUD_PROJECT;
      options.databaseId = process.env.GOOGLE_FIRESTORE_DATABASE_ID;
    }

    const bucketName = process.env.GOOGLE_HISTORY_BLOB_BUCKET_NAME;
    this._firestore = new Firestore(options);
    const _storage = new GCPRawStorage(options);
    this._storage_bucket = _storage.bucket(bucketName);

    return q.Promise<void>((resolve, reject) => {
      q.all([
        this._firestore.collection("health").doc("health").set({ status: "healthy" }),
        _storage.createBucket(bucketName, () => {
          return this._storage_bucket.file("health").save("health");
        }),
      ])
        .then(() => resolve())
        .catch((error) => {
          // Ignore errors if entities/buckets already exist
          if (error.code === 409) {
            resolve();
          } else {
            reject(error);
          }
        });
    });
  }

  public addAccount(account: storage.Account): q.Promise<string> {
    account = storage.clone(account);
    account.id = uuidv4();

    return this._setupPromise
      .then(() => {
        return this._firestore.collection("account").doc(account.id).set(account);
      })
      .then(() => account.id)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getAccount(accountId: string): q.Promise<storage.Account> {
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("account").doc(accountId).get();
      })
      .then((doc) => {
        if (!doc.exists) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }
        return doc.data() as storage.Account;
      })
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getAccountByEmail(email: string): q.Promise<storage.Account> {
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("account").where("email", "==", email).get();
      })
      .then((snapshot) => {
        if (snapshot.empty) {
          throw storage.storageError(storage.ErrorCode.NotFound, "The specified e-mail address doesn't represent a registered user");
        }
        return snapshot.docs[0].data() as storage.Account;
      })
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getAccountIdFromAccessKey(accessKey: string): q.Promise<string> {
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("accessKey").where("name", "==", accessKey).get();
      })
      .then((snapshot) => {
        console.log("snapshot: ", snapshot);
        if (snapshot.empty) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }
        const keyData = snapshot.docs[0].data() as storage.AccessKey;
        console.log("keyData: ", keyData);
        if (new Date().getTime() >= keyData.expires) {
          console.log("keyData.expires: ", keyData.expires);
          throw storage.storageError(storage.ErrorCode.Expired, "The access key has expired.");
        }
        return keyData.createdBy;
      })
      .catch(GCPStorage.gcpErrorHandler);
  }

  public updateAccount(email: string, updates: storage.Account): q.Promise<void> {
    return
    // return this._setupPromise
    //   // .then(() => this.getAccountByEmail(email))
    //   .then(() => {
    //     return this._firestore.collection("account").where("email", "==", email).get();
    //   })
    //   .then((snapshot) => {
    //     // const accountKey = this._datastore.key(["Account", account.id]);
    //     // const updateData = {
    //     //   key: accountKey,
    //     //   data: {
    //     //     gitHubId: updates.gitHubId,
    //     //   },
    //     // };
    //     // return this._datastore.save(updateData);
    //     if (snapshot.empty) {
    //       throw storage.storageError(storage.ErrorCode.NotFound);
    //     }
    //     return snapshot.docs[0].ref.update(updates);
    //   })
    //   .then(() => void 0)
    //   .catch(GCPStorage.gcpErrorHandler);
  }

  public addApp(accountId: string, app: storage.App): q.Promise<storage.App> {
    app = storage.clone(app);
    app.id = uuidv4();

    return this._setupPromise
      .then(() => this.getAccount(accountId))
      .then((account) => {
        const collabMap: storage.CollaboratorMap = {};
        collabMap[account.email] = { accountId: accountId, permission: storage.Permissions.Owner };
        app.collaborators = collabMap;

        return this._firestore.collection("app").doc(app.id).set(app);
      })
      .then(() => this.addAppPointer(accountId, app.id))
      .then(() => app)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getApps(accountId: string): q.Promise<storage.App[]> {
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("appPointer").where("accountId", "==", accountId).get();
      })
      .then((snapshot) => {
        if (snapshot.empty) {
          return [];
        }
        const appPointers = snapshot.docs.map((doc) => doc.data() as AppPointer);
        return appPointers;
      })
      .then((appPointers: AppPointer[]) => {
        if (!appPointers.length) return [];
        const appKeys = appPointers.map((p) => this._firestore.collection("app").doc(p.appId));
        return this._firestore.getAll(...appKeys)
      })
      .then((docs) => {
        const apps = docs.map((doc) => doc.data() as storage.App);
        apps.forEach((app) => {
          app.collaborators = app.collaborators || {};
          const collaborator = Object.values<storage.CollaboratorProperties>(app.collaborators).find(
            (c: storage.CollaboratorProperties) => c.accountId === accountId
          );
          if (!collaborator) {
            throw storage.storageError(storage.ErrorCode.NotFound);
          }
          collaborator.isCurrentAccount = true;
        });
        return apps;
      })
      .catch(GCPStorage.gcpErrorHandler);
  }

  private addAppPointer(accountId: string, appId: string): q.Promise<void> {
    const appPointerKey = `${accountId}:${appId}`; // meaningless
    const appPointer = {
      appId: appId,
      accountId: accountId,
    };
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("appPointer").doc(appPointerKey).set(appPointer);
      })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  private removeAppPointer(accountId: string, appId: string): q.Promise<void> {
    const appPointerKey = `${accountId}:${appId}`;
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("appPointer").doc(appPointerKey).delete();
      })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getApp(accountId: string, appId: string): q.Promise<storage.App> {
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("app").doc(appId).get();
      })
      .then((snapshot) => {
        if (!snapshot.exists) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }

        const app = snapshot.data() as storage.App;
        app.collaborators = app.collaborators || {};
        const collaborator = Object.values<storage.CollaboratorProperties>(app.collaborators).find(
          (c: storage.CollaboratorProperties) => c.accountId === accountId
        );
        if (!collaborator) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }
        collaborator.isCurrentAccount = true;

        return app;
      })
      .catch(GCPStorage.gcpErrorHandler);
  }

  public removeApp(accountId: string, appId: string): q.Promise<void> {
    // TODO:
    return q(<void>null);
    // return this._setupPromise
    //   .then(() => this.getApp(accountId, appId))
    //   .then((app) => {
    //     const transaction = this._datastore.transaction();

    //     return transaction
    //       .run()
    //       .then(() => {
    //         const deletePromises = [];

    //         // Delete app pointers for all collaborators
    //         Object.values(app.collaborators).forEach((collab: storage.CollaboratorProperties) => {
    //           const pointerKey = this._datastore.key(["AppPointer", `${collab.accountId}:${appId}`]);
    //           deletePromises.push(transaction.delete(pointerKey));
    //         });

    //         // Delete deployments
    //         const deploymentQuery = this._datastore.createQuery("Deployment").filter("appId", "=", appId);

    //         return this._datastore.runQuery(deploymentQuery).then(([deployments]) => {
    //           deployments.forEach((deployment) => {
    //             const deploymentKey = this._datastore.key(["Deployment", deployment.id]);
    //             deletePromises.push(transaction.delete(deploymentKey));

    //             // Delete deployment history from Cloud Storage
    //             const historyFile = this._storage.bucket(GCPStorage.HISTORY_BLOB_BUCKET_NAME).file(deployment.id);
    //             deletePromises.push(historyFile.delete().catch(() => {}));
    //           });

    //           // Delete the app itself
    //           const appKey = this._datastore.key(["App", appId]);
    //           deletePromises.push(transaction.delete(appKey));

    //           return Promise.all(deletePromises);
    //         });
    //       })
    //       .then(() => transaction.commit());
    //   })
    //   .then(() => void 0)
    //   .catch(GCPStorage.gcpErrorHandler);
  }

  public updateApp(accountId: string, app: storage.App): q.Promise<void> {
    if (!app.id) throw new Error("No app id");

    return this._setupPromise
      .then(() => this.getApp(accountId, app.id))
      .then(() => {
        return this._firestore.collection("app").doc(app.id).set(app);
      })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public transferApp(accountId: string, appId: string, email: string): q.Promise<void> {
    let app: storage.App;
    let targetAccount: storage.Account;
    let isTargetAlreadyCollaborator: boolean;

    return this._setupPromise
      .then(() => {
        const getAppPromise = this.getApp(accountId, appId);
        const getAccountPromise = this.getAccountByEmail(email);
        return q.all([getAppPromise, getAccountPromise]);
      })
      .spread((appResult: storage.App, accountResult: storage.Account) => {
        app = appResult;
        targetAccount = accountResult;

        if (app.collaborators[email]?.accountId === targetAccount.id) {
          throw storage.storageError(storage.ErrorCode.AlreadyExists, "The given account already owns the app.");
        }

        return this.getApps(targetAccount.id);
      })
      .then((targetApps: storage.App[]) => {
        if (storage.NameResolver.isDuplicate(targetApps, app.name)) {
          throw storage.storageError(
            storage.ErrorCode.AlreadyExists,
            `Cannot transfer ownership. An app with name "${app.name}" already exists for the given collaborator.`
          );
        }

        // Update the current owner to be a collaborator
        const currentOwnerEmail = storage.getOwnerEmail(app);
        app.collaborators[currentOwnerEmail].permission = storage.Permissions.Collaborator;

        // set target collaborator as an owner.
        if (app.collaborators[email]) {
          isTargetAlreadyCollaborator = true;
          app.collaborators[email].permission = storage.Permissions.Owner;
        } else {
          isTargetAlreadyCollaborator = false;
          app.collaborators[email] = {
            accountId: targetAccount.id,
            permission: storage.Permissions.Owner,
          };
        }

        return this._firestore.collection("app").doc(app.id).set(app);
      })
      .then(() => {
        if (!isTargetAlreadyCollaborator) {
          return this.addAppPointer(targetAccount.id, app.id);
        }
      })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public addCollaborator(accountId: string, appId: string, email: string): q.Promise<void> {
    let app: storage.App;
    let collaboratorAccount: storage.Account;

    return this._setupPromise
      .then(() => {
        const getAppPromise = this.getApp(accountId, appId);
        const getAccountPromise = this.getAccountByEmail(email);
        return q.all([getAppPromise, getAccountPromise]);
      })
      .spread((appResult: storage.App, accountResult: storage.Account) => {
        app = appResult;
        collaboratorAccount = accountResult;

        if (app.collaborators[email]) {
          throw storage.storageError(storage.ErrorCode.AlreadyExists, "The given account is already a collaborator for this app.");
        }

        app.collaborators[email] = {
          accountId: collaboratorAccount.id,
          permission: storage.Permissions.Collaborator,
        };
        const setAppPromise = this._firestore.collection("app").doc(app.id).set(app);
        const addPointerPromise = this.addAppPointer(collaboratorAccount.id, app.id);
        return q.all<any>([setAppPromise, addPointerPromise]);
      })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getCollaborators(accountId: string, appId: string): q.Promise<storage.CollaboratorMap> {
    return this._setupPromise
      .then(() => this.getApp(accountId, appId))
      .then((app) => app.collaborators)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public removeCollaborator(accountId: string, appId: string, email: string): q.Promise<void> {
    return this._setupPromise
      .then(() => this.getApp(accountId, appId))
      .then((app) => {
        const collaborator = app.collaborators[email];
        if (!collaborator) {
          throw storage.storageError(storage.ErrorCode.NotFound, "The given email is not a collaborator for this app.");
        }

        if (collaborator.permission === storage.Permissions.Owner) {
          throw storage.storageError(storage.ErrorCode.Invalid, "Cannot remove the owner of the app from collaborator list.");
        }

        delete app.collaborators[email];

        return this._firestore.collection("app").doc(app.id).set(app);
      })
      .then(() => {
        return this.removeAppPointer(accountId, appId);
      })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public addDeployment(accountId: string, appId: string, deployment: storage.Deployment): q.Promise<string> {
    deployment = storage.clone(deployment);
    deployment.id = uuidv4();

    return this._setupPromise
      .then(() => this.getApp(accountId, appId))
      .then(() => {
        return this._firestore.collection("deployment").doc(deployment.id).set(deployment);
      })
      .then(() => {
        return this._firestore.collection("deploymentInfo").doc(deployment.key).set({
          appId: appId,
          deploymentId: deployment.id,
        });
      })
      .then(() => {
        // Initialize empty package history
        return this._storage_bucket.file(deployment.id).save("[]");
      })
      .then(() => deployment.id)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getDeployment(accountId: string, appId: string, deploymentId: string): q.Promise<storage.Deployment> {
    var deployment: storage.Deployment;
    return this._setupPromise
      .then(() => this.getApp(accountId, appId))
      .then(() => {
        return this._firestore.collection("deployment").doc(deploymentId).get();
      })
      .then((snapshot) => {
        if (!snapshot.exists) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }
        const deploymentResult = snapshot.data() as storage.Deployment;
        deployment = deploymentResult;
      })
      .then(() => {
        return this._firestore.collection("deploymentInfo").doc(deployment.key).get();
      })
      .then((snapshot) => {
        if (!snapshot.exists) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }
        const pointer = snapshot.data() as DeploymentInfo;
        if (pointer.appId !== appId) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }
        return deployment;
      })
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getDeploymentInfo(deploymentKey: string): q.Promise<storage.DeploymentInfo> {
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("deploymentInfo").doc(deploymentKey).get();
      })
      .then((snapshot) => {
        if (!snapshot.exists) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }
        return snapshot.data() as storage.DeploymentInfo;
      })
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getDeployments(accountId: string, appId: string): q.Promise<storage.Deployment[]> {
    return this._setupPromise
      .then(() => this.getApp(accountId, appId))
      .then(() => {
        return this._firestore.collection("deploymentInfo").where("appId", "==", appId).get();
      })
      .then((snapshot) => {
        const deploymentIds = snapshot.docs.map((doc) => (doc.data() as DeploymentInfo).deploymentId);
        return this._firestore.collection("deployment").where("id", "in", deploymentIds).get();
      })
      .then((snapshot) => {
        return snapshot.docs.map((doc) => doc.data() as storage.Deployment);
      })
      .catch(GCPStorage.gcpErrorHandler);
  }

  public removeDeployment(accountId: string, appId: string, deploymentId: string): q.Promise<void> {
    // TODO:
    return q(<void>null);
    // return this._setupPromise
    //   .then(() => this.getDeployment(accountId, appId, deploymentId))
    //   .then((deployment) => {
    //     const transaction = this._datastore.transaction();
    //     return transaction
    //       .run()
    //       .then(() => {
    //         const deploymentKey = this._datastore.key(["Deployment", deploymentId]);
    //         const deploymentKeyIndexKey = this._datastore.key(["DeploymentKeyIndex", deployment.key]);

    //         return Promise.all([
    //           transaction.delete(deploymentKey),
    //           transaction.delete(deploymentKeyIndexKey),
    //           this._storage
    //             .bucket(GCPStorage.HISTORY_BLOB_BUCKET_NAME)
    //             .file(deploymentId)
    //             .delete()
    //             .catch(() => {}), // Ignore if history file doesn't exist
    //         ]);
    //       })
    //       .then(() => transaction.commit());
    //   })
    //   .then(() => void 0)
    //   .catch(GCPStorage.gcpErrorHandler);
  }

  public updateDeployment(accountId: string, appId: string, deployment: storage.Deployment): q.Promise<void> {
    if (!deployment.id) throw new Error("No deployment id");

    return this._setupPromise
      .then(() => this.getDeployment(accountId, appId, deployment.id))
      .then(() => {
        return this._firestore.collection("deployment").doc(deployment.id).set(deployment);
      })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public commitPackage(accountId: string, appId: string, deploymentId: string, pkg: storage.Package): q.Promise<storage.Package> {
    if (!deploymentId) throw new Error("No deployment id");
    if (!pkg) throw new Error("No package specified");

    pkg = storage.clone(pkg); // pass by value
    let packageHistory: storage.Package[];

    return this._setupPromise
      .then(() => this.getPackageHistory(accountId, appId, deploymentId))
      .then((history) => {
        packageHistory = history;
        pkg.label = this.getNextLabel(packageHistory);
        return this.getAccount(accountId);
      })
      .then((account) => {
        pkg.releasedBy = account.email;

        // Remove rollout from last package
        if (packageHistory.length > 0) {
          packageHistory[packageHistory.length - 1].rollout = null;
        }

        packageHistory.push(pkg);

        // Trim history if too long
        if (packageHistory.length > GCPStorage.MAX_PACKAGE_HISTORY_LENGTH) {
          packageHistory.splice(0, packageHistory.length - GCPStorage.MAX_PACKAGE_HISTORY_LENGTH);
        }

        return this.updatePackageHistory(accountId, appId, deploymentId, packageHistory);
      })
      .then(() => pkg)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getPackageHistory(accountId: string, appId: string, deploymentId: string): q.Promise<storage.Package[]> {
    return this._setupPromise
      .then(() => this.getDeployment(accountId, appId, deploymentId))
      .then(() => {
        return this._storage_bucket.file(deploymentId).download();
      })
      .then(([content]) => JSON.parse(content.toString()))
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getPackageHistoryFromDeploymentKey(deploymentKey: string): q.Promise<storage.Package[]> {
    return this._setupPromise
      .then(() => this.getDeploymentInfo(deploymentKey))
      .then((info) => {
        return this._storage_bucket.file(info.deploymentId).download();
      })
      .then(([content]) => JSON.parse(content.toString()))
      .catch(GCPStorage.gcpErrorHandler);
  }

  public clearPackageHistory(accountId: string, appId: string, deploymentId: string): q.Promise<void> {
    return this._setupPromise
      .then(() => this.getDeployment(accountId, appId, deploymentId))
      .then((deployment) => {
        delete deployment.package;
        this.updateDeployment(accountId, appId, deployment);
      })
      .then(() => {
        return this._storage_bucket.file(deploymentId).save("[]");
      })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public updatePackageHistory(accountId: string, appId: string, deploymentId: string, history: storage.Package[]): q.Promise<void> {
    // If history is null or empty array we do not update the package history, use clearPackageHistory for that.
    if (!history || !history.length) {
      throw storage.storageError(storage.ErrorCode.Invalid, "Cannot clear package history from an update operation");
    }

    return this._setupPromise
      .then(() => this.getDeployment(accountId, appId, deploymentId))
      .then(() => {
        this.updateDeployment(accountId, appId, { id: deploymentId, package: history[history.length - 1] } as storage.Deployment);
      })
      .then(() => {
        return this._storage_bucket.file(deploymentId).save(JSON.stringify(history));
      })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  private getNextLabel(packageHistory: storage.Package[]): string {
    if (packageHistory.length === 0) {
      return "v1";
    }
    const lastLabel = packageHistory[packageHistory.length - 1].label;
    const lastVersion = parseInt(lastLabel.substring(1)); // Trim 'v' from the front
    return "v" + (lastVersion + 1);
  }

  private static gcpErrorHandler(error: any): never {
    let errorCode: storage.ErrorCode;
    let message = error.message;

    if (error.type === "StorageError") {
      throw error; // Re-throw storage errors
    } else if (error.code) {
      switch (error.code) {
        case 404:
        case "ENOENT":
          errorCode = storage.ErrorCode.NotFound;
          break;
        case 409:
          errorCode = storage.ErrorCode.AlreadyExists;
          break;
        case 413:
          errorCode = storage.ErrorCode.TooLarge;
          break;
        case "ETIMEDOUT":
        case "ESOCKETTIMEDOUT":
        case "ECONNRESET":
          errorCode = storage.ErrorCode.ConnectionFailed;
          break;
        default:
          errorCode = storage.ErrorCode.Other;
      }
    } else {
      errorCode = storage.ErrorCode.Other;
    }

    throw storage.storageError(errorCode, message);
  }

  public addBlob(blobId: string, stream: stream.Readable, streamLength: number): q.Promise<string> {
    return this._setupPromise
      .then(() => {
        const file = this._storage_bucket.file(blobId);
        return new Promise((resolve, reject) => {
          stream
            .pipe(file.createWriteStream())
            .on("error", reject)
            .on("finish", () => resolve(blobId));
        });
      })
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getBlobUrl(blobId: string): q.Promise<string> {
    return this._setupPromise
      .then(() => {
        const file = this._storage_bucket.file(blobId);
        return file.getSignedUrl({
          action: "read",
          expires: Date.now() + 15 * 60 * 1000, // URL expires in 15 minutes
        });
      })
      .then(([url]) => url)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public removeBlob(blobId: string): q.Promise<void> {
    return this._setupPromise
      .then(() => {
        return this._storage_bucket.file(blobId).delete();
      })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public addAccessKey(accountId: string, accessKey: storage.AccessKey): q.Promise<string> {
    accessKey = storage.clone(accessKey);
    accessKey.id = uuidv4();
    accessKey.createdBy = accountId;
    accessKey.createdTime = Date.now();

    return this._setupPromise
      .then(() => {
        return this._firestore.collection("accessKey").doc(accessKey.id).set(accessKey);
      })
      .then(() => accessKey.id)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getAccessKey(accountId: string, accessKeyId: string): q.Promise<storage.AccessKey> {
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("accessKey").doc(accessKeyId).get();
      })
      .then((doc) => {
        if (!doc.exists || doc.data().createdBy !== accountId) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }
        return doc.data() as storage.AccessKey;
      })
      .catch(GCPStorage.gcpErrorHandler);
  }

  public getAccessKeys(accountId: string): q.Promise<storage.AccessKey[]> {
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("accessKey").where("createdBy", "==", accountId).get();
      })
      .then((snapshot) => {
        return snapshot.docs.map((doc) => doc.data() as storage.AccessKey);
      })
      .catch(GCPStorage.gcpErrorHandler);
  }

  public removeAccessKey(accountId: string, accessKeyId: string): q.Promise<void> {
    return this._setupPromise
      .then(() => {
        return this._firestore.collection("accessKey").doc(accessKeyId).get();
      })
      .then((doc) => {
        if (!doc.exists || doc.data().createdBy !== accountId) {
          throw storage.storageError(storage.ErrorCode.NotFound);
        }
        return doc.ref.delete();
      })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public updateAccessKey(accountId: string, accessKey: storage.AccessKey): q.Promise<void> {
    return this._setupPromise
    .then(() => {
      return this._firestore.collection("accessKey").doc(accessKey.id).get();
    })
    .then((doc) => {
      if (!doc.exists || doc.data().createdBy !== accountId) {
        throw storage.storageError(storage.ErrorCode.NotFound);
      }
      return doc.ref.update(accessKey as Record<string, any>);
    })
      .then(() => void 0)
      .catch(GCPStorage.gcpErrorHandler);
  }

  public dropAll(): q.Promise<void> {
    // No-op for safety, similar to Azure implementation
    return q(<void>null);
  }
}
