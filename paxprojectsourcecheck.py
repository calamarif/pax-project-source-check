__author__ = 'Callum'
# paxprojectsourcecheck.py - Pax Project Source Check - checks the source of the schema - 
# Make sure you run with a user with access to ALL of the projects
# Python 3.7.3
# Version: 0.2
# Date: Jul 27th 2020

"""
DISCLAIMER:
Paxata may provide sample code from time to time for customer's use in connection with Paxata products and services.
Paxata has and will retain all right, title and interest in and to the sample code, which you may use as part of any
licensed Paxata products or services.  Sample code is provided "AS IS" and Paxata has no support, warranty, indemnity
or other obligation relating to sample code. Paxata will not be liable for any loss of use, lost profits, interruption
of business, lost or inaccurate data, failure of security mechanisms, or other direct, indirect, consequential,
special, exemplary, punitive or other liability related to sample code or its use, whether in contract, tort or any
other legal theory. If the foregoing disclaimer of direct damages is not enforceable, Paxatas entire liability in
connection with any sample code will be limited to fifty dollars ($50).
"""

import requests, json, copy, logging, os, pprint, datetime, sys, re
from requests.auth import HTTPBasicAuth
from itertools import groupby
from paxata_api_call_utilities import AppError
from paxata_api_call_utilities import auth_with_paxata
from paxata_api_call_utilities import gen_rest_url_stem
from paxata_api_call_utilities import delete_library_item
from paxata_api_call_utilities import get_library_data
from operator import itemgetter, attrgetter

# This env variable controls which json config gets used from the env_config.json
env = "TENANT2"
# if config files are named differently then you have to change that information here.
# these config files are compulsory and should have correct config information!!
env_config_filename = "env_config.json"
#script_config_filename = "CleanDL_config.json"
# -----------------------------------------

# Default location over ride for project specific execution config file and project specific log file
env_config_loc = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config")
script_config_file_loc = None
script_log_file_loc = None
pp = pprint.PrettyPrinter(indent=2)
today = datetime.datetime.today()
timestamp_suffix = today.strftime('%Y%m%d%H%M%S')

# read the environment config json
with open(os.path.join(env_config_loc, env_config_filename)) as json_data:
    env_config_json = json.load(json_data)

if env_config_json["DEFAULT_LOG_LOC"]:
    script_log_file_loc = env_config_json["DEFAULT_LOG_LOC"]
else:
    script_log_file_loc = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

# enable project logging.
logger = logging.getLogger()

# Setup a file handler so that we can create a log file.
# the log file name will be script name without the .py extension.
hdlr = logging.FileHandler(os.path.join(script_log_file_loc, "{}_{}.{}".format(os.path.splitext(os.path.basename
                                        (sys.argv[0]))[0], timestamp_suffix, "log")))
formatter = logging.Formatter('%(asctime)-16s [%(levelname)8s] [%(filename)-s:%(lineno)-3s] %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)

# We also configure the console handler so that we can see the progress of the script on the console
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)
logger.addHandler(consoleHandler)

# Configure the logging level here ...
logger.setLevel(logging.DEBUG)

# lets set the script configuration details.
if env_config_json["DEFAULT_CONFIG_LOC"]:
    script_config_file_loc = env_config_json["DEFAULT_CONFIG_LOC"]
else:
    script_config_file_loc = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config")

# report location of important files.
logger.info("Logs available at : {}".format(script_log_file_loc))
logger.info("Project Configuration File: {}".format(os.path.join(script_config_file_loc)))


def check_library_version_consistency(libraryId, library_version, libraryIdWithVersion):
    validation = []
    try:
        str(library_version)
        my_id = libraryIdWithVersion.split("_")[0]
        my_ver = libraryIdWithVersion.split("_")[1]
        if libraryId != my_id:
            validation.append(libraryId + " not equal to " + my_id)
        else:
            #keep the positional aspects of the list (ie libraryId at index 0 and version at 1)
            validation.append("")
        if str(library_version) != my_ver:
                        validation.append(str(library_version) + " not equal to " + my_ver)
        #empty list if valid, if invalid a list of the things that are invalid
    except(AttributeError):
        validation = ["emptyid"]
    return validation

def check_for_invalid_field_types(auth_token, paxata_url, project_schema_json):
    result = False
    for column in project_schema_json:
        if column['columnType']  not in ['Number','String','DateTime','Boolean']:
            print("INCORRECT DATATYPE IN PROJECT= " + column['columnType'])
            result = True
    return result

# (5) Get all the Projects that have been described with a specific "description_tag"
def get_all_project_information(auth_token, paxata_url):
    #Package_Tagged_Projects = []
    #package_counter = 0
    #max_num_of_projects = 0
    list_of_projects = []
    url_request = (paxata_url + "/projects")
    my_response = requests.get(url_request,auth=auth_token , verify=True)
    if(my_response.ok):
        list_of_projects = json.loads(my_response.content)
        #for item in jDataProjectIds:
        #    if description_tag == jDataProjectIds[package_counter].get('description'):
        #        ProjectNames.append(jDataProjectIds[package_counter].get('name'))
        #        Package_Tagged_Projects.append(jDataProjectIds[package_counter].get('projectId'))
        #        max_num_of_projects += 1
        #    package_counter += 1
    return list_of_projects

def get_metadata_of_datasource(auth_token,paxata_url,libraryId,library_version):
    url_request = (paxata_url + "/library/data/"+str(libraryId)+"/"+str(library_version))
    try:
        my_response = requests.get(url_request, auth=auth_token, verify=True)
        jdata_datasources = []
        if(my_response.ok):
            jdata_datasources = json.loads(my_response.content)
    except requests.exceptions.RequestException as e:  # Timed out
        logger.error("REQUEST TIMED OUT {}".format(str(url_request)))
        jdata_datasources = ["problem with "+ url_request]
    return jdata_datasources

def get_project_script(auth_token,paxata_url,projectId):
    url_request = (paxata_url + "/scripts?projectId=" + projectId + "&version=" + "-1")
    json_of_project = []
    try:
        myResponse = requests.get(url_request, auth=auth_token)
        if (myResponse.ok):
            # Loads (Load String) takes a Json file and converts into python data structure (dict or list, depending on JSON)
            json_of_project = json.loads(myResponse.content)
        else:
            json_of_project = 0
            myResponse.raise_for_status()
    except requests.exceptions.RequestException as e:  # Timed out
        json_of_project.append("timeout")
    return(json_of_project)

# Update an existing Project's script file
def update_project_with_new_script(auth_token,paxata_url,updated_json_script,projectId):
    url_request = (paxata_url + "/scripts?update=script&force=true&projectId=" + str(projectId))
    s = {'script': json.dumps(updated_json_script)}
    myResponse = requests.put(url_request, data=s, auth=auth_token)
    result = False
    if (myResponse.ok):
        # json_of_existing_project = json.loads(myResponse.content)
        result = True
    else:
        #if there is a problem in updating the project, it would indicate a problem with the script, so lets output it
        print(myResponse.content)
        result = False
    return result

def run_a_project(auth_token,paxata_url,projectId):
    post_request = (paxata_url + "/project/publish?projectId=" + projectId)
    postResponse = requests.post(post_request, auth=auth_token)
    if (postResponse.ok):
        print("Project Run - " + projectId)
    else:
        print("Something went wrong with POST call " + str(postResponse))
    # I need to investigate the below, sometimes postResponse.content is a dict, sometimes a list, hence the two below trys
    try:
        AnswersetId = json.loads(postResponse.content)[0].get('dataFileId')
    except(AttributeError):
        AnswersetId = json.loads(postResponse.content).get('dataFileId', 0)
    return AnswersetId

# (3) POST Library data from Paxata and load it into a JSON structure
def get_paxata_library_data(auth_token,paxata_url,library_dataset_id):
    post_request = (paxata_url + "/datasource/exports/local/" + library_dataset_id + "?format=json")
    post_response = requests.post(post_request,auth=auth_token)
    JsonData = ""
    if (post_response.ok):
        JsonData = json.loads(post_response.content)
    return JsonData

def update_project_with_new_dataset(auth_token,paxata_url,updated_json_script,projectId):
    url_request = (paxata_url + "/rest/scripts?update=datasets&force=true&projectId=" + str(projectId))
    s = {'script': json.dumps(updated_json_script)}
    myResponse = requests.put(url_request, data=s, auth=auth_token)
    result = False
    print(myResponse)
    if (myResponse.ok):
        # json_of_existing_project = json.loads(myResponse.content)
        result = True
    else:
        #if there is a problem in updating the project, it would indicate a problem with the script, so lets output it
        print(myResponse.content)
        result = False
    return result

def check_project():
    # *****************THESE ARE YO VARIABLES - YOU NEED TO EDIT THESE *******
    try:
        paxata_url = gen_rest_url_stem(**env_config_json[env])
    except AppError as e:
        logger.error('REST URL stem cannot be created. Program terminated. {}'.format(e.msg))
        raise SystemExit("Program terminating abruptly ...")
    try:
        auth_token = auth_with_paxata(**env_config_json[env])
    except AppError as e:
        logger.error('Auth object cannot be created. Program terminated. {}'.format(e.msg))
        raise SystemExit("Program terminating abruptly ...")

    #1 get the projects in the tenant
    list_of_projects = get_all_project_information(auth_token, paxata_url)
    #list to hold the problem projectIds
    problem_projectIds = []
    print ("Number of Projects = " + str(len(list_of_projects)))
    logger.info('Paxata - Metadata report for {}. Number of Projects: {}'.format(paxata_url, str(len(list_of_projects))))

    for project_json in list_of_projects:
        problem_project_flag = False
        projectId = project_json.get('projectId')
        project_name = project_json.get('name')
        project_creator = project_json.get('userName')
        project_script = get_project_script(auth_token,paxata_url,projectId)
        try:
            if project_script[0] == "timeout":
                logger.error("Timeout Error {} (ProjectId {}) timed out".format(project_name,projectId))
        except KeyError as e:
            # Not doing anything with this for now
            error = e 
        #take a copy of the project so we can export out the entire script
        full_json_script = copy.deepcopy(project_script)        
        # main loop to go through each project
        for step in project_script['steps']:
            if (step['type'] == "AnchorTable" or step['type'] == "LookupTable" ):
                # initialise the library_schema from previous run
                library_schema = ""
                project_data_version_inconsistency = ""
                # treat LookupTables and AnchorTables slightly differently
                if step['type'] == "AnchorTable":
                    libraryId = step['importStep']['libraryId']
                    library_version = step['importStep']['libraryVersion']
                    project_schema = step['importStep']['columns']
                    libraryIdWithVersion = step['importStep']['libraryIdWithVersion']
                if step['type'] == "LookupTable":
                    libraryId = step['steps'][0]['libraryId']
                    library_version = step['steps'][0]['libraryVersion']
                    project_schema = step['steps'][0]['columns']
                    libraryIdWithVersion = step['steps'][0]['libraryIdWithVersion']
                
                project_data_version_inconsistency = check_library_version_consistency(libraryId, library_version,libraryIdWithVersion)
                # if this isn't empty, there is an inconsistency
                if not project_data_version_inconsistency:
                    logger.warning('Issues with id and version consistency: {}.'.format(project_data_version_inconsistency))
                    problem_project_flag = True

                # this is a function that will check that there are only valid values in field type (in the project script) not necessary
                # Neha advised I don't need this.
                # valid_datatypes_in_project = check_for_invalid_field_types(auth_token, paxata_url, project_schema)
                # Get the project metadata
                jdata_datasources = get_metadata_of_datasource(auth_token,paxata_url,libraryId,library_version)                
                # checking this first because there is the possibility that the schema is empty and everything else is populated
                if jdata_datasources:
                    # extract the specific library data for 
                    library_name = jdata_datasources.get('name')
                    library_schema = jdata_datasources.get('schema')
                    library_createTime = jdata_datasources.get('createTime')
                    library_source = jdata_datasources.get('source').get('type')
                    library_size = jdata_datasources.get('size')
                    library_rowcount = jdata_datasources.get('rowCount')
                    library_state = jdata_datasources.get('state')
                
                # Check for a deleted or no existing library
                if not library_schema:
                    logger.warning('Schema is MISSING for Project: {}.  WARNING: Dataset no longer exists or has not finished importing into the Library'.format(project_name))
                    problem_project_flag = True

                else:
                    # remove two unnecessary metadata items from the library metadata
                    for field in library_schema:
                        try:
                            field.pop('columnTags')
                            field.pop('maxSize')
                            # rename library metadata to have the same keys as the project dict
                            field['columnType'] = field.pop('type')
                            field['columnName'] = field.pop('name')
                            field['columnDisplayName'] = field.pop('orignalColumnName')
                            # Some library "Hidden" data has None, some has False. Standardising it here to False.
                            if field['hidden'] == None:
                                field['hidden'] = False
                        except:
                            logger.error('Something went wrong manipulating the library schema {}.'.format(library_source))

                    # Check if the schema's are different
                    if project_schema == library_schema:
                        logger.info('Schemas are the same for Project: {}'.format(project_name))
                    else:
                        differences_made_by_paxata = False
                        i =0
                        while len(project_schema) > i:
                            set1 = set(project_schema[i].items())
                            set2 = set(library_schema[i].items())
                            differences = set1 ^ set2

                            # check for an empty set and don't print it.
                            if differences:
                                # this is catering for the fact that Paxata auto add a "(number)" at the end of a column, adding this
                                #logic in, to reduce the number of false positives in the output.                                
                                previous_m = ""
                                column_with_parenthesis = ""
                                first_columname_without_parenthesis = ""
                                counter = 0
                                try:
                                    for x in differences:
                                        counter += 1
                                        m = re.search(r'\(\d+\)$', str(x[1]))
                                        if m:
                                            previous_m = m
                                            column_with_parenthesis = x
                                        else:
                                            if not first_columname_without_parenthesis:
                                                #Assume this was the first entry of the pair, so we'll keep it for later
                                                first_columname_without_parenthesis = str(x[1])
                                            else:
                                                #This was the second time around, so set it to blank again (no parenthesis)
                                                first_columname_without_parenthesis = ""
                                        # if we have parenthesis and another value let's check if Paxata renamed it
                                        if first_columname_without_parenthesis and column_with_parenthesis:
                                            # we have two columns, one with parenthesis removed, so lets see if they're really different or Paxata renamed them
                                            temp_var = column_with_parenthesis[1][0:-len(previous_m.group())].strip()
                                            if not (str(first_columname_without_parenthesis).strip() == temp_var):
                                                # Columns are not the same, but one has parenthesis in it
                                                logger.warning("Difference Project Script and Library Item (one has parentheses) = {} and {} ".format(temp_var,first_columname_without_parenthesis))
                                            else:
                                                differences_made_by_paxata = True
                                                logger.info("OLD Differences between Project Script and Library Item that Paxata probably made= {} ".format(differences))
                                        else:
                                            if not counter % 2:
                                                # This was the second run, no parenthesis (They're really different)
                                                logger.warning("Difference between Project Column= {} and Library Column= {} ".format(temp_var,x))       
                                except:
                                    logger.error('Some kind of error occurred logging project'.format(str(differences)))
                            i+=1
                        if not differences_made_by_paxata:
                            # add the projectId to the list of things that might have a problem
                            logger.warning("LIBRARY SCHEMA = {}".format(str(library_schema)))
                            logger.warning("PROJECT SCHEMA = {}".format(str(project_schema)))
                            # not sure if I need this, but will keep it for now.
                            logger.warning("PROJECT METADATA = {}".format(str(full_json_script)))
                            logger.warning("Schemas are different for Project: {} and Library: {} (Version: {}). Created:{}".format(project_name, library_name, str(library_version), library_createTime))
                            logger.warning("From library source: {} . Rows = {}  ( {}  bytes). State: {}".format(library_source, str(library_rowcount), str(library_size), library_state))
                                
                        # Also want to check for different display name and column name
                        for column in project_schema:
                            differences_made_by_paxata = False
                            if not column['columnDisplayName'] == column['columnName']:
                                display_name = re.search(r'\(\d+\)$', str(column['columnDisplayName']))
                                column_name = re.search(r'\(\d+\)$', str(column['columnName']))
                                if column_name and display_name:
                                    if str(column['columnName'])[0:-len(column_name.group())].strip() == column['columnDisplayName'] or str(column['columnName'])[0:-len(column_name.group())].strip() == str(column['columnDisplayName'])[0:-len(display_name.group())].strip():
                                        #differences are parenthesise only
                                        differences_made_by_paxata = True
                                if column_name and not display_name:
                                    if str(column['columnName'])[0:-len(column_name.group())].strip() == column['columnDisplayName']:
                                        differences_made_by_paxata = True
                                if display_name and not column_name:
                                    if str(column['columnName'])== str(column['columnDisplayName'])[0:-len(display_name.group())].strip():
                                        differences_made_by_paxata = True
                                if not differences_made_by_paxata:                                
                                    logger.warning("Intra Column Check - manually changed Project script metadata. Column:  {}  is not equal to: {}, in Project Name= {}  (ProjectId= {})".format(column['columnDisplayName'], column['columnName'], project_name,  projectId ))
                            if differences_made_by_paxata == True:
                                logger.info("Intra Column Check - paxata likely changed Project script metadata. Column:  {}  is not equal to: {}, in Project Name= {}  (ProjectId= {})".format(column['columnDisplayName'], column['columnName'], project_name,  projectId ))
                        problem_project_flag = True
        #append problem projects to a list to be summarised at the end   
        if problem_project_flag:
            problem_projectIds.append(projectId) 
    logger.warning("Summary. Projects analysed = {}. Projects with problems = {} ({}%)".format(len(list_of_projects),len(problem_projectIds),round(((len(problem_projectIds)/len(list_of_projects)) * 100))))
    logger.error("Potential problem projectIds are: {}".format(str(problem_projectIds)))

if __name__ == "__main__":
    check_project()
