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

import requests


def get_poll(poll, base_link='https://governance-portal-v2.now.sh'):

    response = requests.get(base_link + '/api/polling/all-polls')
    if response.status_code != 200:
        # something went wrong...
        print('API error {}.'.format(response.status_code))
        title = options = None
        exists = False
    else:
        content = response.json()
        polls = {poll['pollId']: (poll['title'], poll['options']) for poll in content}
        if poll in polls:
            title, options = polls[poll]
            exists = True
        else:
            title = 'Unknown poll'
            options = None
            exists = False

    return exists, title, options


def get_all_polls(base_link='https://governance-portal-v2.now.sh'):

    response = requests.get(base_link + '/api/polling/all-polls')
    if response.status_code != 200:
        # something went wrong...
        print('API error {}.'.format(response.status_code))
        polls = dict()
    else:
        content = response.json()
        polls = {str(poll['pollId']): (poll['title'], poll['options']) for poll in content}

    return polls
