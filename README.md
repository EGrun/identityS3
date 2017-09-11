# Identity S3

Uses Serverless to create an AWS lambda API wrapper around an S3 bucket that will autoincrement identities of posted entities. Does not guarantee isolated commits, ensure that only one writer is given access to the API to prevent overwriting.
