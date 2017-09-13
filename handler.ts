import {S3Service} from './s3Service';
import {IdentityS3Post, IdentityS3Put} from './identityS3';

class WebUtils {
  public static withContext(event: any, callback: any) {
    return {
      error: (message, errorCode: number = 500) => {
        console.error(`Error occurred: ${message}. Provided event data: ` + JSON.stringify(event));
        callback(null, WebUtils.generateResponse(500, message));
      },
      success: (message: string) => {
        callback(null, WebUtils.generateResponse(200, message))
      }
    }
  }

  public static generateResponse(statusCode: number, message: string) {
    return {
      'statusCode': statusCode,
      headers: {
        "Access-Control-Allow-Origin" : "*", // Required for CORS support to work
        "Access-Control-Allow-Credentials" : true // Required for cookies, authorization headers with HTTPS
      },
      body: JSON.stringify({
        'result': message
      })
    }
  }
}

function parseMetadata(keys: any, headers: any) : any
{ 
  //metadata_keys are read from request header and passed through and stored to s3 file metadata

  let metadata = [];

  if (!keys || !headers){
    return metadata;
  }

  for (let key of keys) {    
    if (headers[key]) {
      metadata[<string>key] = headers[key];
    }
  }
  return metadata;
}

export async function post(event, context, callback) {

  const s3Bucket = <string>process.env.S3_BUCKET;
  const s3Prefix = <string>process.env.S3_PREFIX;
  const s3Extension = <string>process.env.S3_EXTENSION;
  const acl = <string>process.env.S3_OBJECT_ACL;
  const identityKey = <string>process.env.IDENTITY_KEY;
  const metadataKeys = <string>process.env.METADATA_KEYS ? (<string>process.env.METADATA_KEYS).split(";") : null;
  const metadata = parseMetadata(metadataKeys, event.headers);

  //verify entity
  const entity = JSON.parse(event.body);
  if (!entity ) {
    WebUtils.withContext(event, callback).error("Invalid entity passed in", 400);
    return;
  }
  
  try {
    const s3Service = new S3Service(s3Bucket);
    new IdentityS3Post(entity, s3Service, s3Prefix, s3Extension, identityKey, acl, metadata).post().then(
      fulfill => WebUtils.withContext(event, callback).success(fulfill[identityKey]),
      rejected => WebUtils.withContext(event, callback).error(rejected, 500));
  }
  catch (e)
  {
    WebUtils.withContext(event, callback).error(e, 500);
  }
}

export async function put(event, context, callback) {

  const s3Bucket = <string>process.env.S3_BUCKET;
  const s3Prefix = <string>process.env.S3_PREFIX;
  const s3Extension = <string>process.env.S3_EXTENSION;
  const acl = <string>process.env.S3_OBJECT_ACL;
  const identityKey = <string>process.env.IDENTITY_KEY;
  const metadataKeys = <string>process.env.METADATA_KEYS ? (<string>process.env.METADATA_KEYS).split(";") : null;
  const metadata = parseMetadata(metadataKeys, event.headers);

  //verify entity
  const entity = JSON.parse(event.body);
  if (!entity ) {
    WebUtils.withContext(event, callback).error("Invalid entity passed in", 400);
    return;
  }

  //verify id
  if (!event.pathParameters || !event.pathParameters[identityKey] ) {
    WebUtils.withContext(event, callback).error(`Missing identifier: ${identityKey}`, 400);
    return;
  }
  const entityId = event.pathParameters[identityKey];
console.log(`id: [${entityId}] - entityId: [${entity[identityKey]}]`);

  if (entityId != entity[identityKey])
  {
    WebUtils.withContext(event, callback).error(`Provided Id [${entityId}] does not match identity key on entity [${entity[identityKey]}].`, 400);
    return;
  }

  try {
    const s3Service = new S3Service(s3Bucket);
    new IdentityS3Put(entityId, entity, s3Service, s3Prefix, s3Extension, identityKey, acl, metadata).put().then(
      fulfill => {
        //todo: publish to SNS
        WebUtils.withContext(event, callback).success("OK");
      },
      rejected => WebUtils.withContext(event, callback).error(rejected, 500));
  }
  catch (e) {
    WebUtils.withContext(event, callback).error(e, 500);
  }
}
