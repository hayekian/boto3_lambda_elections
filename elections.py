import requests
import boto3
import xml.etree.ElementTree as ET
import json
import sys
import StringIO;
from boto3.dynamodb.conditions import Key, Attr


def lambda_handler(event, context):

    print json.dumps(event) ;
    if (event['name'] == "JorgeReload"):
        UpdateSystem();
        return "System Updated";
    else:
        return GetItems(event['race'], event['name'], event['state']);


def GetItems(RaceType, name, state):
   dynamodb = boto3.resource('dynamodb', region_name='us-east-1');
   table = dynamodb.Table('Candidates');
   fe = None;
   if (RaceType is not None and len(RaceType) > 0):
       fe = Attr('race').eq(RaceType)
   if (name is not None and len(name) > 0):
       if (fe is not None):
           fe = fe & Attr('Name').contains(name.upper());
       else:
           fe = Attr('Name').contains(name.upper());
   if (state is not None and len(state) > 0):
       if (fe is not None):
           fe = fe & Attr('state').contains(state.upper());
       else:
           fe = Attr('state').contains(state.upper());
   response = None;
   if (fe is None):
       response = table.scan();
   else:
       response = table.scan(FilterExpression=fe);

   items = response['Items'];

   return items;


def UpdateSystem():

    # first we delete/recreate dynamodb table
    client = boto3.client('dynamodb')
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1');
    try:
        table = dynamodb.Table('Candidates');
        if table is not None:
            print 'deleting table';
            table.delete();
    except:
        pass

    # wait for table to be deleted
    waiter = client.get_waiter('table_not_exists');
    waiter.wait(TableName='Candidates');


    print 'creating table';
    table = dynamodb.create_table(
        TableName='Candidates',
        KeySchema=[
        {
            'AttributeName': 'CandidateID',
            'KeyType': 'HASH' #Partition ke
        },
        {
            'AttributeName': 'Name',
            'KeyType': 'RANGE' #Sort key
        }
        ],
        AttributeDefinitions=[
        {
            'AttributeName': 'CandidateID',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'Name',
            'AttributeType': 'S'
        },
        ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 15,
        'WriteCapacityUnits': 15
    }
    );

    print("Table status:", table.table_status)

    #wait for table to be created
    waiter = client.get_waiter('table_exists');
    waiter.wait(TableName='Candidates');
    print("Table has been created");

    print ("getting election data from gov site");
    s3 = boto3.resource('s3');
    # get the URL of government data from a settings file
    #{
    #"DataURL" : "http://www.fec.gov/data/CandidateSummary.do?format=xml&election_yr=2016"
    #}
    DataURL = s3.Object('hayekian-settings','Elections.json').get()["Body"].read();
    obj = json.loads(DataURL);
    # fetch file....
    xmlfile = requests.get(obj["DataURL"]).content;

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('Candidates')
    root = ET.fromstring(xmlfile);

    with table.batch_writer() as batch:

        for candidate in root.iter('can_sum'):
            print candidate.find('can_nam').text;
            batch.put_item(
        Item={ 'CandidateID' : candidate.find('can_id').text,
        'Name': candidate.find('can_nam').text,
        'state':candidate.find('can_sta').text,
        'link':candidate.find('lin_ima').text,
        'race':candidate.find('can_off').text,
        'district': candidate.find('can_off_dis').text,
        'party':candidate.find('can_par_aff').text,
        'incumbent' : candidate.find('can_inc_cha_ope_sea').text

               }
                );

    print 'Data has been loaded';
    return;


