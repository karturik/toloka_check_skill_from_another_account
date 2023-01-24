import pandas as pd
import requests
import toloka.client as toloka
from tqdm.notebook import tqdm as tqdm


URL_WORKER = 'https://toloka.yandex.ru/requester/worker/'
URL_API = "https://toloka.yandex.ru/api/v1/"
OAUTH_TOKEN_ie = ''
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN_ie, "Content-Type": "application/JSON"}
toloka_client = toloka.TolokaClient(OAUTH_TOKEN_ie, 'PRODUCTION')
skill_id = ''

# GET LIST OF WORKERS WITH SKILL
workers_with_skill = requests.get(f'https://toloka.dev/api/v1/user-skills?skill_id={skill_id}&limit=1000', headers=HEADERS).json()
list_workers_with_skill = []
for worker_data in workers_with_skill['items']:
    print(worker_data)
    user_id = worker_data['user_id']
    list_workers_with_skill.append(user_id)
print(list_workers_with_skill)


OAUTH_TOKEN_1 = ''
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN_1, "Content-Type": "application/JSON"}
toloka_client = toloka.TolokaClient(OAUTH_TOKEN_1, 'PRODUCTION')

list_of_projects = []

project_ids = []
status = 'OPEN'

# GET LIST OF POOLS IN PROJECT
for project_id in project_ids:
    pools = requests.get(f'https://toloka.dev/api/v1/pools?project_id={project_id}&status={status}', headers=HEADERS).json()
    print(pools)

    # GET ALL WORKERS FROM POOLS
    full_assignment_df = pd.DataFrame(columns=['ASSIGNMENT:assignment_id', 'ASSIGNMENT:worker_id'], data=None)

    for pool_data in pools['items']:
        print(pool_data)
        pool_id = pool_data['id']
        df_toloka = toloka_client.get_assignments_df(pool_id, status=['SUBMITTED'])
        df_toloka = df_toloka[['ASSIGNMENT:assignment_id', 'ASSIGNMENT:worker_id']]
        full_assignment_df = pd.concat([full_assignment_df, df_toloka])

    #CHECK IF WORKER IS IN LIST OF WORKERS WITH SKILL
    for i in tqdm(full_assignment_df['ASSIGNMENT:worker_id']):
        print('Check worker: ', i)
        if i in list_workers_with_skill:
            try:
                print('Worker has skill, reject set')
                assignment_id = full_assignment_df[full_assignment_df['ASSIGNMENT:worker_id']==i]['ASSIGNMENT:assignment_id'].values[0]
                print('Set ID: ', assignment_id)
                toloka_client.reject_assignment(assignment_id=assignment_id, public_comment='One user can send only one set')
                print('Set rejected')
                print('--------------------------------------------------')
                break
            except Exception as e:
                if 'IncorrectActionsApiError' in e:
                    print('Cant reject set - there is another status')
                else:
                    print(e)
        else:
            print('Worker without skill')
