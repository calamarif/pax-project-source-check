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

import requests
import json
import csv
import re
import logging
# import sys
# import pprint
# import os
# import copy
# import logging
# import datetime
# import errno
# import time

from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from contextlib import closing

# create logger
logger = logging.getLogger(__name__)

# suppress insecure request warning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Error class, so that we can raise exceptions from the function.
class AppError(Exception):
    def __init__(self, msg, err_code):
        self.msg = msg
        self.errCode = err_code

class ApiEp:
    PAX_AUTHENTICATION_TOKEN_API = r'/tokens'
    PAX_USERS_API = r'/users'
    PAX_GROUPS_API = r'/groups'
    PAX_GROUP_MEMBERSHIP_API = r'/group/users'
    PAX_LIBRARY_DATA_API = r'/library/data'
    PAX_LIBRARY_PUBLISH_API = r'/library/publish'
    PAX_LIBRARY_TAGS_API = r'/library/tags'
    PAX_LIBRARY_EXPORT_API = r'/library/exports'
    PAX_PROJECTS_API = r'/projects'
    PAX_PROJECT_PUBLISH_API = r'/project/publish'
    PAX_SCRIPTS_API = r'/scripts'
    PAX_SESSIONS_API = r'/sessions'
    PAX_ROLES_API = r'/roles'
    PAX_TENANTS_API = r'/tenants'
    PAX_GUARD_RAIL_LIMITS_API = r'/guardrails/limits'
    PAX_SYSTEM_JOB_API = r'/systemjob'
    PAX_TIERS_API = r'/tiers'
    PAX_LDAP_CONFIGURATION_API = r'/ldapconfig'
    PAX_LDAP_CONFIGURATION_VERIFICATION_API = r'/ldapconfig/verify'
    PAX_SAML_IDP_CONFIGURATION_API = r'/samlidpmds'
    PAX_SAML_SP_CONFIGURATION_API = r'/samlspmds'
    PAX_NOTIFICATIONS_API = r'/notifications'
    PAX_DATA_SOURCE_IMPORTS_API = r'/datasource/imports'
    PAX_DATA_SOURCE_EXPORTS_API = r'/datasource/exports'
    PAX_CONNECTOR_FACTORY_API = r'/connector/factories'
    PAX_CONNECTOR_FIELDS_API = r'/connector/fields'
    PAX_CONNECTOR_CONFIGURATION_API = r'/connector/configs'
    PAX_DATA_SOURCE_CONFIGURATION_API = r'/datasource/configs'
    PAX_DATA_SOURCE_FIELDS_API = r'/datasource/fields'
    PAX_PROPERTY_ENCRYPTION_API = r'/cipher'
    PAX_ENCRYPTION_SERVICE_MANAGEMENT_API = r'/encryption'
    PAX_DATASET_PERMISSIONS_API = r'/permissions/dataset'
    PAX_PROJECT_PERMISSIONS_API = r'/permissions/project'

    def __init__(self):
        pass


# Misc functions
def gen_rest_url_stem(**kwargs):
    """
    Generate the stem of rest url. The API end points will be appended to this stem
    :param kwargs:
    :return:
    """
    protocol = kwargs["PROTOCOL"]
    paxata_core_server = kwargs["PAXATA_CORE_SERVER"]
    paxata_app_port = kwargs["PAXATA_APP_PORT"]
    paxata_api_root = kwargs["PAXATA_API_ROOT"]

    url_regex = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    if paxata_app_port:
        rest_url_stem = r"{}://{}:{}/{}".format(protocol,
                                            paxata_core_server,
                                            paxata_app_port,
                                            paxata_api_root)
    else:
        rest_url_stem = r"{}://{}/{}".format(protocol,
                                            paxata_core_server,
                                            paxata_api_root)

    if url_regex.match(rest_url_stem):
        # base url to which API endpoints will be appended
        return rest_url_stem
    else:
        raise AppError('Non-valid url: {}'.format(rest_url_stem), -1)


def auth_with_paxata(**kwargs):
    """
    Setup basic auth to connect to the tenant in Paxata
    If using REST Token from Paxata make username an empty string and assign the
    REST Token to the password argument of HTTPBasicAuth method
    :param kwargs:
    :return:
    """

    paxata_rest_token = kwargs["PAXATA_REST_TOKEN"]
    paxata_login_name = kwargs["PAXATA_LOGIN_NAME"]
    paxata_login_password = kwargs["PAXATA_LOGIN_PASSWORD"]

    if paxata_rest_token:
        basic_auth = HTTPBasicAuth('', paxata_rest_token)
    elif paxata_login_name and paxata_login_password:
        basic_auth = HTTPBasicAuth(paxata_login_name,paxata_login_password)
    else:
        raise AppError('Usage: Provide either a REST token or login name and password.', -1)

    return basic_auth


# REST operation functions ...
def get_delimited_list_of_return_attributes(**kwargs):
    """
    This function will return a list of tuple. The tuple will be delimited value returned by Paxata REST call
    :param kwargs:
    :return:
    """
    url = kwargs["url"]
    requests_param = kwargs["requests_param"]
    basic_auth = kwargs["basic_auth"]
    filter_regex = re.compile(kwargs["filter_regex"])

    try:
        with closing(requests.get(url, params=requests_param, stream=True, auth=basic_auth, verify=False)) as r:
            if r.status_code != 200:
                raise AppError("HTTP error encountered. status code: {}, reason: {}. \
                                For latest version of file use version value -1.".format(r.status_code, r.reason),
                               r.status_code)
            else:
                reader = csv.reader(r.iter_lines(), delimiter=',', quotechar='"')
                attribute_list = []
                if reader:
                    for row in reader:
                        if filter_regex.match(row[0].rstrip()):
                            attribute_list.append(tuple(row[0].rstrip().split("\t")))
                        else:
                            pass

    except requests.ConnectionError as ce:
        raise AppError("Connection error. Please validate the URL provided: "
                       + ce.request.url
                       + "\nStack: \n\t"
                       + str(ce.args[0].message), ce.errno)
    else:
        return attribute_list


def get_json(**kwargs):
    """
    This function will do the get call and will return feedback as a json output.
    :param kwargs:
    :return:
    """
    url = kwargs["url"]
    requests_param = kwargs["requests_param"]
    basic_auth = kwargs["basic_auth"]

    try:
        response = requests.request("GET", url,
                                    auth=basic_auth,
                                    params=requests_param,
                                    verify=False)
        if response.status_code != 200:
            raise AppError("HTTP error encountered. status code: {}, reason: {}. \
                            For latest version of file use version value -1.".format(response.status_code,
                                                                                     response.reason),
                           response.status_code)
        else:
            pass
        try:
            out_json = json.loads(response.text)
        except ValueError as ve:
            raise AppError('No valid JSON returned by GET {}'.format(url)
                           + "\nStack: \n\t"
                           + str(ve.args[0]), -1)
    except requests.ConnectionError as ce:
        raise AppError("Connection error. Please validate the URL provided: "
                       + ce.request.url
                       + "\nStack: \n\t"
                       + str(ce.args[0].message), ce.errno)

    return out_json


def rest_post(**kwargs):
    """
    This function will do the POST call.
    :param kwargs:
    :return:
    """
    url = kwargs["url"]
    basic_auth = kwargs["basic_auth"]
    logger.debug("Executing Function: {}".format(rest_post.__name__))
    logger.debug("... arg url: {}".format(url))
    logger.debug("... arg dict: {}".format(kwargs))

    try:
        if "data" in kwargs and "file" not in kwargs:
            my_data = kwargs["data"]
            response = requests.request("POST", url,
                                        auth=basic_auth,
                                        data=my_data,
                                        verify=False)
        elif "data" in kwargs and "file" in kwargs:
            my_data = kwargs["data"]
            files = {'data': open(kwargs["file"],'rb')}
            response = requests.request("POST", url,
                                        auth=basic_auth,
                                        data=my_data,
                                        files=files,
                                        verify=False)
        elif "requests_param" in kwargs:
            requests_param = kwargs["requests_param"]
            response = requests.request("POST", url,
                                        auth=basic_auth,
                                        params=requests_param,
                                        verify=False)
        else:
            raise AppError('Post function call lacks valid arguments.', -1)

        if response.status_code != 200:
            raise AppError("HTTP error encountered. status code: {}, reason: {}.".format(response.status_code,
                                                                                         response.reason),
                           response.status_code)
        else:
            pass

        try:
            out_json = json.loads(response.text)
        except ValueError as ve:
            raise AppError('No valid JSON returned by GET {}'.format(url)
                           + "\nStack: \n\t"
                           + str(ve.args[0]), -1)
    except requests.ConnectionError as ce:
        raise AppError("Connection error. Please validate the URL provided: "
                       + ce.request.url
                       + "\nStack: \n\t"
                       + str(ce.args[0].message), ce.errno)

    return out_json


def rest_put(**kwargs):
    """
    This function will do the PUT call
    :param kwargs:
    :return:
    """
    url = kwargs["url"]
    basic_auth = kwargs["basic_auth"]
    logger.debug("Executing Function: {}".format(rest_post.__name__))
    logger.debug("... arg url: {}".format(url))
    logger.debug("... arg dict: {}".format(kwargs))

    try:
        if "data" in kwargs and "file" not in kwargs:
            my_data = kwargs["data"]
            response = requests.request("PUT", url,
                                        auth=basic_auth,
                                        data=my_data,
                                        verify=False)
        elif "data" in kwargs and "file" in kwargs:
            my_data = kwargs["data"]
            files = {'data': open(kwargs["file"], 'rb')}
            response = requests.request("PUT", url,
                                        auth=basic_auth,
                                        data=my_data,
                                        files=files,
                                        verify=False)
        elif "requests_param" in kwargs:
            requests_param = kwargs["requests_param"]
            response = requests.request("PUT", url,
                                        auth=basic_auth,
                                        params=requests_param,
                                        verify=False)
        else:
            raise AppError('Post function call lacks valid arguments.', -1)

        if response.status_code != 200:
            raise AppError("HTTP error encountered. status code: {}, reason: {}.".format(response.status_code,
                                                                                         response.reason),
                           response.status_code)
        else:
            pass

        try:
            out_json = json.loads(response.text)
        except ValueError as ve:
            raise AppError('No valid JSON returned by GET {}'.format(url)
                           + "\nStack: \n\t"
                           + str(ve.args[0]), -1)
    except requests.ConnectionError as ce:
        raise AppError("Connection error. Please validate the URL provided: "
                       + ce.request.url
                       + "\nStack: \n\t"
                       + str(ce.args[0].message), ce.errno)

    return out_json


def rest_delete(**kwargs):
    """
    This function will do the DELETE call.
    :param kwargs:
    :return:
    """
    url = kwargs["url"]
    requests_param = kwargs["requests_param"]
    basic_auth = kwargs["basic_auth"]
    logger.debug("Executing Function: {}".format(rest_delete.__name__))
    logger.debug("... arg url: {}".format(url))
    logger.debug("... arg dict: {}".format(kwargs))
    try:
        response = requests.request("DELETE", url,
                                    auth=basic_auth,
                                    params=requests_param,
                                    verify=False)
        if response.status_code != 200:
            raise AppError("HTTP error encountered. status code: {}, reason: {}.".format(response.status_code,
                                                                                         response.reason),
                           response.status_code)
        else:
            pass

        try:
            out_json = json.loads(response.text)
        except ValueError as ve:
            raise AppError('No valid JSON returned by GET {}'.format(url)
                           + "\nStack: \n\t"
                           + str(ve.args[0]), -1)
    except requests.ConnectionError as ce:
        raise AppError("Connection error. Please validate the URL provided: "
                       + ce.request.url
                       + "\nStack: \n\t"
                       + str(ce.args[0].message), ce.errno)

    return out_json


# Get functions to retrieve information from Paxata

# get the json representing all the projects in the system.
def get_projects(**kwargs):
    """
    Gets the json provided by paxata rest api of all projects in the tenant.
    :param kwargs:
    :return: parsed json
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    api_endpoint = ApiEp.PAX_PROJECTS_API
    requests_param = {"pretty": "true"}  # parameter to be passed to requests http call.

    if "return_attributes" in kwargs:
        return_attributes = kwargs["return_attributes"]
        requests_param.update({"return": return_attributes})
    else:
        return_attributes = None

    if "project_name" in kwargs:
        requests_param.update({"name": kwargs["project_name"]})
    else:
        pass

    if "user_id" in kwargs:
        requests_param.update({"userId": kwargs["user_id"]})
    else:
        pass

    if "project_id" in kwargs:
        api_url = "{}{}/{}".format(rest_url_stem, api_endpoint, kwargs["project_id"])
    else:
        api_url = "{}{}".format(rest_url_stem, api_endpoint)

    if "filter_regex" in kwargs:
        filter_regex = kwargs["filter_regex"]
    else:
        filter_regex = ".*"

    if return_attributes:
        try:
            delimited_list_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
                "filter_regex": filter_regex
            }
            output = get_delimited_list_of_return_attributes(**delimited_list_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)
    else:
        try:
            get_json_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
            }
            output = get_json(**get_json_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)

    return output


# get paxata user metadata
def get_users(**kwargs):
    """
    Gets the json provided by paxata rest api of all projects in the tenant.
    :param kwargs:
    :return: parsed json
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    api_endpoint = ApiEp.PAX_USERS_API
    requests_param = {"pretty": "true"}  # parameter to be passed to requests http call.

    if "return_attributes" in kwargs:
        return_attributes = kwargs["return_attributes"]
        requests_param.update({"return": return_attributes})
    else:
        return_attributes = None

    if "user_name" in kwargs:
        requests_param.update({"name": kwargs["user_name"]})
    else:
        pass

    if "email" in kwargs:
        requests_param.update({"email": kwargs["email"]})
    else:
        pass

    if "auth_token" in kwargs:
        requests_param.update({"authToken": kwargs["auth_token"]})
    else:
        pass

    if "user_id" in kwargs:
        api_url = "{}{}/{}".format(rest_url_stem, api_endpoint, kwargs["user_id"])
    else:
        api_url = "{}{}".format(rest_url_stem, api_endpoint)

    if "filter_regex" in kwargs:
        filter_regex = kwargs["filter_regex"]
    else:
        filter_regex = ".*"

    if return_attributes:
        try:
            delimited_list_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
                "filter_regex": filter_regex
            }
            output = get_delimited_list_of_return_attributes(**delimited_list_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)
    else:
        try:
            get_json_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
            }
            output = get_json(**get_json_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)

    return output


def get_groups(**kwargs):
    """
    This function will return group information from Paxata API
    :param kwargs:
    :return:
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    requests_param = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_GROUPS_API

    if "return_attributes" in kwargs:
        return_attributes = kwargs["return_attributes"]
        requests_param.update({"return": return_attributes})
    else:
        return_attributes = None

    if "group_name" in kwargs:
        requests_param.update({"groupName": kwargs["group_name"]})
    else:
        pass

    if "group_type" in kwargs:
        requests_param.update({"groupType": kwargs["group_type"]})
    else:
        pass

    if "group_id" in kwargs:
        api_url = "{}{}/{}".format(rest_url_stem, api_endpoint, kwargs["group_id"])
    else:
        api_url = "{}{}".format(rest_url_stem, api_endpoint)

    if "filter_regex" in kwargs:
        filter_regex = kwargs["filter_regex"]
    else:
        filter_regex = ".*"

    if return_attributes:
        try:
            delimited_list_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
                "filter_regex": filter_regex
            }
            output = get_delimited_list_of_return_attributes(**delimited_list_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)
    else:
        try:
            get_json_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
            }
            output = get_json(**get_json_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)

    return output


def get_library_data(**kwargs):
    """
    This function gets the library object details
    :param kwargs:
    :return:
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    requests_param = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_LIBRARY_DATA_API

    if "return_attributes" in kwargs:
        return_attributes = kwargs["return_attributes"]
        requests_param.update({"return": return_attributes})
    else:
        return_attributes = None

    if "version" in kwargs:
        requests_param.update({"version": kwargs["version"]})
    else:
        pass

    if "state" in kwargs:
        requests_param.update({"state": kwargs["state"]})
    else:
        pass

    if "data_file_id" in kwargs:
        api_url = "{}{}/{}".format(rest_url_stem, api_endpoint, kwargs["data_file_id"])
    else:
        api_url = "{}{}".format(rest_url_stem, api_endpoint)

    if "filter_regex" in kwargs:
        filter_regex = kwargs["filter_regex"]
    else:
        filter_regex = ".*"

    if return_attributes:
        try:
            delimited_list_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
                "filter_regex": filter_regex
            }
            output = get_delimited_list_of_return_attributes(**delimited_list_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)
    else:
        try:
            get_json_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
            }
            output = get_json(**get_json_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)

    return output


def get_library_tags(**kwargs):
    """
    This function will return tag data from Paxata
    :param kwargs:
    :return:
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    requests_param = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_LIBRARY_TAGS_API

    if "return_attributes" in kwargs:
        return_attributes = kwargs["return_attributes"]
        requests_param.update({"return": return_attributes})
    else:
        return_attributes = None

    if "version" in kwargs:
        requests_param.update({"version": kwargs["version"]})
    else:
        pass

    if "data_file_id" in kwargs:
        requests_param.update({"dataFileId": kwargs["data_file_id"]})
    else:
        pass

    if "tagId" in kwargs:
        api_url = "{}{}/{}".format(rest_url_stem, api_endpoint, kwargs["tagId"])
    else:
        api_url = "{}{}".format(rest_url_stem, api_endpoint)

    if "filter_regex" in kwargs:
        filter_regex = kwargs["filter_regex"]
    else:
        filter_regex = ".*"

    if return_attributes:
        try:
            delimited_list_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
                "filter_regex": filter_regex
            }
            output = get_delimited_list_of_return_attributes(**delimited_list_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)
    else:
        try:
            get_json_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
            }
            output = get_json(**get_json_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)

    return output


def get_project_script(**kwargs):
    """
    This function will get the project json script from Paxata
    :param kwargs:
    :return:
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    requests_param = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_SCRIPTS_API

    if "return_attributes" in kwargs:
        return_attributes = kwargs["return_attributes"]
        requests_param.update({"return": return_attributes})
    else:
        return_attributes = None

    if "project_id" in kwargs:
        requests_param.update({"projectId": kwargs["project_id"]})
    else:
        pass

    if "version" in kwargs:
        api_url = "{}{}/{}".format(rest_url_stem, api_endpoint, kwargs["version"])
    else:
        api_url = "{}{}".format(rest_url_stem, api_endpoint)

    if "filter_regex" in kwargs:
        filter_regex = kwargs["filter_regex"]
    else:
        filter_regex = ".*"

    if return_attributes:
        try:
            delimited_list_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
                "filter_regex": filter_regex
            }
            output = get_delimited_list_of_return_attributes(**delimited_list_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)
    else:
        try:
            get_json_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
            }
            output = get_json(**get_json_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)

    return output


def get_publish_project_item(**kwargs):
    """
    This function will get all the items published by a project
    :param kwargs:
    :return:
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    requests_param = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_PROJECT_PUBLISH_API

    if "return_attributes" in kwargs:
        return_attributes = kwargs["return_attributes"]
        requests_param.update({"return": return_attributes})
    else:
        return_attributes = None

    if "project_id" in kwargs:
        requests_param.update({"projectId": kwargs["project_id"]})
    else:
        pass

    if "filter_regex" in kwargs:
        filter_regex = kwargs["filter_regex"]
    else:
        filter_regex = ".*"

    api_url = "{}{}".format(rest_url_stem, api_endpoint)

    if return_attributes:
        try:
            delimited_list_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
                "filter_regex": filter_regex
            }
            output = get_delimited_list_of_return_attributes(**delimited_list_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)
    else:
        try:
            get_json_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "requests_param": requests_param,
            }
            output = get_json(**get_json_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)

    return output


# functions to change things in paxata, add, update and delete
def publish_project(**kwargs):
    """
    This function will publish a Paxata project to generate the answerset.
    :param kwargs:
    :return:
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    requests_param = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_PROJECT_PUBLISH_API

    if "project_id" in kwargs:
        requests_param.update({"projectId": kwargs["project_id"]})
    else:
        pass

    if "name" in kwargs:
        requests_param.update({"name": kwargs["name"]})
    else:
        pass

    if "description" in kwargs:
        requests_param.update({"description": kwargs["description"]})
    else:
        pass

    if "all" in kwargs:
        requests_param.update({"all": kwargs["all"]})
    else:
        pass

    if "lens" in kwargs:
        requests_param.update({"lens": kwargs["lens"]})
    else:
        pass

    api_url = "{}{}".format(rest_url_stem, api_endpoint)

    try:
        rest_post_args = {
            "url": api_url,
            "basic_auth": basic_auth,
            "requests_param": requests_param,
        }
        output = rest_post(**rest_post_args)
    except AppError as e:
        raise AppError(e.msg, e.errCode)

    return output


# update a project script.
def project_script_update(**kwargs):
    """
    This function will update the project script
    :param kwargs:
    :return:
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    data = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_SCRIPTS_API

    if "project_id" in kwargs:
        data.update({"projectId": kwargs["project_id"]})
    else:
        pass

    if "update" in kwargs:
        data.update({"update": kwargs["update"]})
    else:
        pass

    if "datasets" in kwargs:
        data.update({"datasets": kwargs["datasets"]})
    else:
        pass

    if "force" in kwargs:
        data.update({"force": kwargs["force"]})
    else:
        pass

    if "script" in kwargs:
        data.update({"script": kwargs["script"]})
    else:
        pass

    if "version" in kwargs:
        api_url = "{}{}/{}".format(rest_url_stem, api_endpoint, kwargs["version"])
    else:
        api_url = "{}{}".format(rest_url_stem, api_endpoint)

    try:
        rest_put_args = {
            "url": api_url,
            "basic_auth": basic_auth,
            "data": data,
        }
        output = rest_put(**rest_put_args)
    except AppError as e:
        raise AppError(e.msg, e.errCode)

    return output


# update permission on dataset
def dataset_permission_update(**kwargs):
    """
    This function will update the project script
    :param kwargs:
    :return:
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    data = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_DATASET_PERMISSIONS_API

    if "data_file_id" in kwargs:
        api_url = "{}{}/{}".format(rest_url_stem, api_endpoint, kwargs["data_file_id"])
    else:
        errmsg = 'data file id is a required parameter'
        errcode = 1
        raise AppError(errmsg, errcode)

    if "version" in kwargs:
        api_url = '{}/{}'.format(api_url, kwargs["version"])
    else:
        pass

    if "user_id" in kwargs:
        data.update({"userId": kwargs["user_id"]})
    else:
        pass

    if "group_id" in kwargs:
        data.update({"groupId": kwargs["group_id"]})
    else:
        pass

    if kwargs["group_id"] or kwargs["user_id"]:
        pass
    else:
        errmsg = 'groupId or userId is a required parameter'
        errcode = 1
        raise AppError(errmsg, errcode)

    if "reset" in kwargs:
        data.update({"reset": kwargs["reset"]})
    else:
        pass

    if "grant" in kwargs:
        data.update({"grant": kwargs["grant"]})
    else:
        pass

    if "revoke" in kwargs:
        data.update({"revoke": kwargs["revoke"]})
    else:
        pass

    try:
        rest_put_args = {
            "url": api_url,
            "basic_auth": basic_auth,
            "data": data,
        }
        output = rest_put(**rest_put_args)
    except AppError as e:
        raise AppError(e.msg, e.errCode)

    return output


def import_delimited_from_local(**kwargs):
    """
    This function will import a delimited file from local filesystem.
    This function only implements subset of options available in paxata to do such import.
    This function will evolve as new options are supported.
    :param kwargs:
    :return:
    """
    data_source_id = "local"
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    data = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_DATA_SOURCE_IMPORTS_API

    if "item_path" in kwargs:
        data.update({"itemPath": kwargs["item_path"]})
    else:
        pass

    if "import" in kwargs:
        data.update({"import": kwargs["import"]})
    else:
        pass

    if "format" in kwargs:
        data.update({"format": kwargs["format"]})
    else:
        pass

    if "name" in kwargs:
        data.update({"name": kwargs["name"]})
    else:
        pass

    if "description" in kwargs:
        data.update({"description": kwargs["description"]})
    else:
        pass

    if "header_lines" in kwargs:
        data.update({"headerLines": kwargs["header_lines"]})
    else:
        pass

    if "skip_data_lines" in kwargs:
        data.update({"skipDataLines": kwargs["skip_data_lines"]})
    else:
        pass

    if "ignore_lines" in kwargs:
        data.update({"ignoreLines": kwargs["ignore_lines"]})
    else:
        pass

    if "process_quotes" in kwargs:
        data.update({"processQuotes": kwargs["process_quotes"]})
    else:
        pass

    if "value_separator" in kwargs:
        data.update({"valueSeparator": kwargs["value_separator"]})
    else:
        pass

    if "file_to_upload" not in kwargs:
        raise AppError('Valid file path is required.', -1)
    else:
        pass

    if "data_file_id" in kwargs:
        api_url = "{}{}/{}/{}".format(rest_url_stem, api_endpoint, data_source_id, kwargs["data_file_id"])
        try:
            delimited_import_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "data": data,
                "file": kwargs["file_to_upload"]
            }
            output = rest_put(**delimited_import_args)
        except AppError as e:
            raise AppError(e.msg, e.errCode)
    else:
        api_url = "{}{}/{}".format(rest_url_stem, api_endpoint, data_source_id)
        try:
            delimited_import_args = {
                "url": api_url,
                "basic_auth": basic_auth,
                "data": data,
                "file": kwargs["file_to_upload"]
            }
            output = rest_post(**delimited_import_args)
        except:
            raise AppError(e.msg, e.errCode)

    return output


def export_delimited_file_to_local(**kwargs):
    """
    The intention of this function is to export a paxata answerset to local filesystem.
    This function will only publish delimited files.
    :param kwargs:
    :return:
    """
    data_source_id = "local"
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    data = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_DATA_SOURCE_EXPORTS_API

    if "item_path" in kwargs:
        data.update({"itemPath": kwargs["item_path"]})
    else:
        pass

    if "file_name" in kwargs:
        data.update({"fileName": kwargs["fileName"]})
    else:
        pass

    if "format" in kwargs:
        data.update({"format": kwargs["format"]})
    else:
        data.update({"format": "separator"})
        logger.debug("Format defaulted to separator")

    if "include_header" in kwargs:
        data.update({"includeHeader": kwargs["include_header"]})
    else:
        logger.debug("Defaulting to include header lines.")

    if "line_separator" in kwargs:
        data.update({"lineSeparator": kwargs["line_separator"]})
    else:
        logger.debug("Defaulting to newline.")

    if "quote_values" in kwargs:
        data.update({"quoteValues": kwargs["quote_values"]})
    else:
        logger.debug("Defaulting to not quoting values.")

    if "value_separator" in kwargs:
        data.update({"valueSeparator": kwargs["value_separator"]})
    else:
        data.update({"valueSeparator": ","})
        logger.debug("Defaulting to comma as value separator.")

    if "data_file_id" in kwargs:
        api_url = "{}{}/{}/{}".format(rest_url_stem, api_endpoint, data_source_id, kwargs["data_file_id"])
    else:
        raise AppError("Data File Id is required parameter and was not provided.", -1)

    if "version" in kwargs:
        api_url = "{}/{}".format(api_url, kwargs["version"])
    else:
        logger.debug("Answerset version was not explicitly provided. Publishing the latest version.")

    try:
        delimited_export_args = {
            "url": api_url,
            "basic_auth": basic_auth,
            "data": data
        }
        output = rest_post(**delimited_export_args)
    except:
        raise AppError(e.msg, e.errCode)

    return output


def export_delimited_file_to_s3(**kwargs):
    """
    The intention of this function is to export a paxata answerset to local filesystem.
    This function will only publish delimited files.
    :param kwargs:
    :return:
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    data = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_DATA_SOURCE_EXPORTS_API

    if "data_source_id" in kwargs:
        data_source_id = kwargs["data_source_id"]
    else:
        raise AppError("Data source id is compulsory", -1)

    if "item_path" in kwargs:
        data.update({"itemPath": kwargs["item_path"]})
    else:
        pass

    if "file_name" in kwargs:
        data.update({"fileName": kwargs["fileName"]})
    else:
        pass

    if "format" in kwargs:
        data.update({"format": kwargs["format"]})
    else:
        data.update({"format": "separator"})
        logger.debug("Format defaulted to separator")

    if "include_header" in kwargs:
        data.update({"includeHeader": kwargs["include_header"]})
    else:
        logger.debug("Defaulting to include header lines.")

    if "line_separator" in kwargs:
        data.update({"lineSeparator": kwargs["line_separator"]})
    else:
        logger.debug("Defaulting to newline.")

    if "quote_values" in kwargs:
        data.update({"quoteValues": kwargs["quote_values"]})
    else:
        logger.debug("Defaulting to not quoting values.")

    if "value_separator" in kwargs:
        data.update({"valueSeparator": kwargs["value_separator"]})
    else:
        data.update({"valueSeparator": ","})
        logger.debug("Defaulting to comma as value separator.")

    if "data_file_id" in kwargs:
        api_url = "{}{}/{}/{}".format(rest_url_stem, api_endpoint, data_source_id, kwargs["data_file_id"])
    else:
        raise AppError("Data File Id is required parameter and was not provided.", -1)

    if "version" in kwargs:
        api_url = "{}/{}".format(api_url, kwargs["version"])
    else:
        logger.debug("Answerset version was not explicitly provided. Publishing the latest version.")

    try:
        delimited_export_args = {
            "url": api_url,
            "basic_auth": basic_auth,
            "data": data
        }
        output = rest_post(**delimited_export_args)
    except:
        raise AppError(e.msg, e.errCode)

    return output


def delete_tags(**kwargs):
    """
    This function will delete tags
    :param kwargs:
    :return:
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    requests_param = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_LIBRARY_TAGS_API

    if "tagId" in kwargs:
        api_url = "{}{}/{}".format(rest_url_stem, api_endpoint, kwargs["tagId"])
    else:
        pass

    try:
        rest_delete_args = {
            "url": api_url,
            "basic_auth": basic_auth,
            "requests_param": requests_param
        }
        output = rest_delete(**rest_delete_args)
    except AppError as e:
        raise AppError(e.msg, e.errCode)

    return output


def delete_library_item(**kwargs):
    """
    This function will delete library item
    :param kwargs:
    :return:
    """
    rest_url_stem = kwargs["rest_url_stem"]
    basic_auth = kwargs["basic_auth"]

    requests_param = {"pretty": "true"}  # parameter to be passed to requests http call.
    api_endpoint = ApiEp.PAX_LIBRARY_DATA_API

    if "data_file_id" in kwargs:
        api_url = "{}{}/{}".format(rest_url_stem, api_endpoint, kwargs["data_file_id"])
    else:
        raise AppError(" Function {} cannot be called without a data file id".format(delete_library_item.__name__), -1)

    if "version" in kwargs:
        api_url = "{}/{}".format(api_url, kwargs["version"])
    else:
        pass

    try:
        rest_delete_args = {
            "url": api_url,
            "basic_auth": basic_auth,
            "requests_param": requests_param
        }
        output = rest_delete(**rest_delete_args)
    except AppError as e:
        raise AppError(e.msg, e.errCode)

    return output


def main():
    pass

if __name__ == "main":
    main()
