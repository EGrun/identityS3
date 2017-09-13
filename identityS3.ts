import {S3Service} from './s3Service';
import * as _ from 'lodash'

/**
 * Create new entities in the S3 bucket stored to a numeric key based location in an S3 bucket.
 * Autoincrements the identifier to the next available numeric S3 key.
 * Will add the identifier to the entity data.
 * 
 * @export
 * @class IdentityS3Post
 */
export class IdentityS3Post {

  private _s3Service: S3Service;
  private _prefix: string;
  private _extension: string;
  private _identityKey: string;
  private _entity: any;
  private _metadata: any;
  private _acl: string;

  /**
   * Creates an instance of IdentityS3Post.
   * @param {*} entity Json data of the entity to store
   * @param {S3Service} s3Service S3 Service
   * @param {string} prefix Prefix or folder path to store entities, must include trailing delimiter
   * @param {string} extension File extension to store entities, i.e. '.json'
   * @param {string} identityKey Identity will be stored to this location on the entity data
   * @param {string} acl ACL for items stored to S3
   * @param {*} [metadata] Optional, metadata that will be stored on the file at S3
   * @memberof IdentityS3Post
   */
  constructor(entity: any, s3Service: S3Service, prefix: string, extension: string, identityKey: string, acl: string, metadata?: any) {

    if (!s3Service) {
      throw "Invalid s3Service.";
    }
    if (!extension) {
      throw "Invalid extension.";
    }
    if (!entity) {
      throw "Invalid entity.";
    }
    if (!acl) {
      throw "Invalid S3 ACL";
    }

    this._s3Service = s3Service;
    this._prefix = prefix;
    this._extension = extension;
    this._identityKey = identityKey;
    this._entity = entity;
    this._acl = acl;
    this._metadata = metadata;
  }

  /**
   * Execute the post command on S3
   * 
   * @returns {Promise<any>}
   * @memberof IdentityS3Post
   */
  public post() : Promise<any> {
    return new Promise((resolve, reject) => {
      
      //determine next id
      this._s3Service.listObjectNames(this._prefix).then(files => {

            //remove extension and prefix, order by numeric desc
            let highestId = _(files)
            .map((f) => parseInt(f.substring(this._prefix.length)))
            .filter((f) => f)
            .orderBy((f) => f, 'desc')
            .head();

            let nextId = (highestId || 0) + 1;
            console.log(`nextId:[${nextId}]`);

            //add id to json entity
            if (this._identityKey)
            {
                this._entity[this._identityKey] = nextId;
            }

            //persist entity
            this._s3Service.putObject(
                nextId+this._extension, 
                JSON.stringify(this._entity), 
                this._acl,
                false,
                this._prefix,
                this._metadata)
                .then(
                    resolved => {resolve(this._entity)}, 
                    rejected => {reject(rejected)})}, 

        rejected => {reject(rejected)})});
  }
}

/**
 * Replace entity at specified id location with new data.
 * 
 * @export
 * @class IdentityS3Put
 */
export class IdentityS3Put {

  private _s3Service: S3Service;
  private _prefix: string;
  private _extension: string;
  private _identityKey: string;
  private _id: number;
  private _entity: any;
  private _acl: string;
  private _metadata: any;

  /**
   * Creates an instance of IdentityS3Put.
   * @param {number} id 
   * @param {*} entity Json data of the entity to store
   * @param {S3Service} s3Service S3 Service
   * @param {string} prefix Prefix or folder path to store entities, must include trailing delimiter
   * @param {string} extension File extension to store entities, i.e. '.json'
   * @param {string} identityKey Identity will be stored to this location on the entity data
   * @param {string} acl ACL for items stored to S3
   * @param {*} [metadata] Optional, metadata that will be stored on the file at S3
   * @memberof IdentityS3Put
   */
  constructor(id: number, entity: any, s3Service: S3Service, prefix: string, extension: string, identityKey: string, acl: string, metadata?: any) {

    if (!s3Service) {
      throw "Invalid s3Service.";
    }
    if (!extension) {
      throw "Invalid extension.";
    }      
    if (!id || id <= 1) {
      throw "Id must be greater than 0.";
    }
    if (!entity) {
      throw "Invalid entity.";
    }
    if (!acl) {
      throw "Invalid S3 ACL";
    }
    if (identityKey && id != entity[identityKey])
    {
      throw `Provided Id [${id}] does not match identity key on entity [${entity[identityKey]}].`;
    }

    this._s3Service = s3Service;
    this._prefix = prefix;
    this._extension = extension;
    this._identityKey = identityKey;
    this._id = id;
    this._entity = entity;
    this._acl = acl;
    this._metadata = metadata;
  }

  /**
   * Execute the put command on S3
   * 
   * @returns {Promise<any>} 
   * @memberof IdentityS3Put
   */
  public put() : Promise<any> {

    return new Promise((resolve, reject) => {

      let s3Key = this._id+this._extension;

      //confirm s3 key exists
      this._s3Service.exists(s3Key, this._prefix).then(exists => 
      {
        if (!exists) {
          reject(`S3 key [${(this._prefix || '') + s3Key}] not found.`)
        }

        //persist entity
        this._s3Service.putObject(
            s3Key, 
            JSON.stringify(this._entity), 
            this._acl, 
            true,
            this._prefix,
            this._metadata)
            .then(
                resolved => {resolve(this._entity)},
                rejected => {reject(rejected)})

      }, rejected => {reject(rejected)})

    });
  }
}