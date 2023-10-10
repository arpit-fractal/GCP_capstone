# GCP_capstone
 
## To login and authenticate.
`gcloud auth login`

## To generate the user end authentication credentials.
`gcloud auth application-default login`

## Displays data associated with the Compute Engine project resource.
`gcloud compute project-info describe --project [PROJECT_ID]`
`gcloud projects describe [PROJECT_ID]`
`gcloud config get-value project`

## Set the default project ID. Can use --project flag everytime as an alternative.
`gcloud config set project [PROJECT_ID]`

## Print the current region/zone
`gcloud config get-value compute/region`
`gcloud config get-value compute/zone`

## Set the current region/zone
`gcloud config set compute/region REGION`
`gcloud config set compute/zone ZONE`