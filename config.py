#  Copyright 2021 DAI Foundation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
from dotenv import load_dotenv


PROJECT_ROOT = os.path.realpath(os.path.dirname(__file__))

# load secrets from the local .env file
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

# Blockchain node connection
NODE = os.environ.get('BC_NODE')

# TokenFlow relational Data Warehouse connection
TF_DB_CONNECTION = dict(host=os.environ.get('TF_DB_HOST'), port=os.environ.get('TF_DB_PORT'),
                        database=os.environ.get('TF_DB_NAME'), user=os.environ.get('TF_DB_USER'),
                        password=os.environ.get('TF_DB_PASS'))

# Google BigQuery connection
BQ_PROJECT = 'mcd-265409'
BQ_CREDENTIALS = os.path.join(PROJECT_ROOT, os.environ.get('BQ_CREDENTIALS'))

# Snowflake connection
SNOWFLAKE_CONNECTION = dict(account=os.environ.get('SNOWFLAKE_ACCOUNT'), user=os.environ.get('SNOWFLAKE_USER'),
                            password=os.environ.get('SNOWFLAKE_PASS'), role=os.environ.get('SNOWFLAKE_ROLE'),
                            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'))

API_TOKEN = os.environ.get('API_PUBLIC_TOKEN')
GUI_PASSWORD1 = os.environ.get('GUI_PASSWORD1')
GUI_PASSWORD2 = os.environ.get('GUI_PASSWORD2')
# GUI_PASSWORD3 = os.environ.get('GUI_PASSWORD3')
