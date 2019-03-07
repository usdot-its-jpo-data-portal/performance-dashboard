import boto3
import yaml
from botocore.exceptions import ClientError

def sendEmail(scriptName, e):

    try:
        with open("config.yml", 'r') as stream:
            config = yaml.load(stream)

        BODY_TEXT = (e)
        client = boto3.client('ses',region_name=config["aws_ses_region"])

        response = client.send_email(
            Destination={
                'ToAddresses': [
                    config["reporting_recipient"],
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': "UTF-8",
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': "UTF-8",
                    'Data': "Metrics Server: An error occurred while executing the " + scriptName + " metrics script.",
                },
            },
            Source=config["reporting_sender"]
        )
    # Display an error if something goes wrong. 
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:")
        print(response['MessageId'])
