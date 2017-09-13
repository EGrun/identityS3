import * as AWS from 'aws-sdk';
import * as _ from 'lodash'

/**
 * wrapper for aws s3 that support Promises
 * 
 * @export
 * @class S3Service
 */
export class S3Service {

  private _s3Bucket: string;
  private _s3 : AWS.S3; 

  constructor(s3Bucket: string) {
    if (!s3Bucket) {
      throw "Invalid S3 Bucket";
    }

    this._s3Bucket = s3Bucket;
  }

  public listObjectNames(prefix?: string) : Promise<any> {

    const maxKeys = 2147483647;
    const delimiter = '/';

    return new Promise((resolve, reject) => {
      try {

        let files = [];

        let f = function (data) {

          files = files.concat(_.map(data.Contents, 'Key'));

          if (data.IsTruncated) {

            this._s3.listObjectsV2({
              Bucket: this._s3Bucket,
              MaxKeys: maxKeys,
              Delimiter: delimiter,
              Prefix: prefix,
              ContinuationToken: data.NextContinuationToken
            }, (err, data) => {
            if (err) {
              reject("Error calling listObjectsV2 on S3. Error: " + err);
            } else {
              f.call(this, data);
            }});
          }
          else
          {
            resolve(files);
          }
          
        }

        this._connect();

        this._s3.listObjectsV2({
          Bucket: this._s3Bucket,
          MaxKeys: maxKeys,
          Delimiter: delimiter,
          Prefix: prefix,
          StartAfter: prefix
        }, (err, data) => {
            if (err) {
              reject("Error calling listObjectsV2 on S3. Error: " + err);
            } else {
              f.call(this, data);
            }});

      } catch (e) {
        reject("Exception occurred in S3Service::listObjectNames(): " + e);
      }
    });
  }

  public exists(key: string, prefix?: string) : Promise<any>  {
    return new Promise((resolve, reject) => {
      try {

      this._connect();
      
      this._s3.headObject({
        Bucket: this._s3Bucket,
        Key: (prefix || '') + key,
      }, (err, data) => {
          if (err) {
            resolve(false);
          } else {
            resolve(true);
          }});

      } catch (e) {
        reject("Exception occurred in S3Service::exists(): " + e);
      }
    });
  }

  public putObject(key: string, body: string, acl: string, keepMetadata: boolean, prefix?: string, metadata?: any) : Promise<any>  {
    return new Promise((resolve, reject) => {
      try {

      this._connect();

      //TODO: if keepMetadata, retrieve current list of metadata keys and merge with metadata object

      this._s3.putObject({
        Bucket: this._s3Bucket,
        Body: body,
        Key: (prefix || '') + key,
        ACL: acl,
        Metadata: (metadata || null)
      }, (err, data) => {
          if (err) {
            reject("Error calling putObject on S3. Error: " + err);
          } else {
            resolve();
          }});

      } catch (e) {
        reject("Exception occurred in S3Service::putObject(): " + e);
      }
    });
  }

  private _connect() {
    if (!this._s3) {
      this._s3 = new AWS.S3({params: {Bucket: this._s3Bucket}});
    }
  }

}