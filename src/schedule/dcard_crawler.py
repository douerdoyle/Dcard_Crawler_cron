if __name__ == '__main__':
    import sys
    sys.path.append('../')
import json, os, requests, time, traceback
from pprint   import pprint
from copy     import deepcopy
from datetime import datetime, timedelta
from collections      import OrderedDict
from urllib.parse     import urlparse
from sqlalchemy       import asc, desc, or_
from lib.dcard_tools  import RequestDcardByRESTfulAPI
from lib.tools        import format_datetime_dict, format_datetime_list, get_my_ip, pop_dict_empty_value_key, check_duplicate_process
from lib.email_sender import GmailSender
from lib.es.elastic   import Elastic
from models.dcard_forums import DcardForums
from settings.environment import app, db

db.create_all()
run_hours_limit = 23
retry_n_limit = 2
class dcard_crawler():
    def __init__(self):
        self.rdbra = RequestDcardByRESTfulAPI()
        self.es = Elastic(
            host=app.config['ES_SETTING']['CONNECTION']['HOST'], 
            port=app.config['ES_SETTING']['CONNECTION']['PORT'], 
            username=app.config['ES_SETTING']['CONNECTION']['ACCOUNT'],
            password=app.config['ES_SETTING']['CONNECTION']['PASSWORD']
            )
        # for index_category, index_info in app.config['ES_SETTING']['ES_INDEX'].items():
        #     self.es.create_index(index_info['INDEX_NAME'], index_info['MAPPING_FILEPATH'])

        self.article_es_key_list = []
        f = open(app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['MAPPING_FILEPATH'], 'r')
        for key in json.loads(f.read())['mappings']['properties']:
            self.article_es_key_list.append(key)

        self.comment_es_key_list = []
        f = open(app.config['ES_SETTING']['ES_INDEX']['COMMENT']['MAPPING_FILEPATH'], 'r')
        for key in json.loads(f.read())['mappings']['properties']:
            self.comment_es_key_list.append(key)

        self.exist_index = {}

    ################################################################################
    # 工具區
    def crawler_run_over_multi_hours(self, start_time, hours=run_hours_limit):
        return(True if (start_time+timedelta(hours=hours))<=datetime.now() else False)

    def gen_article_url(self, forum_alias, article_id):
        return('https://www.dcard.tw/f/{}/p/{}'.format(forum_alias, article_id))

    def batch_load_retryer(self, input_batch_load_list):
        # 為避免ES主機無法連線，這邊先用while迴圈測試能否bulk存入
        retry_n = 0
        while retry_n<retry_n_limit:
            try:
                self.es.batch_load(input_batch_load_list)
                break
            except:
                retry_n+=1
        # 如果測試兩次後，再試第三次，第三次還出錯，就會自動raise並寄信
        if retry_n>=retry_n_limit:
            self.es.batch_load(input_batch_load_list)

    def format_dcard_article(self, input_dict):
        input_dict = format_datetime_dict(input_dict)
        dictionary = {
            '_id'        : input_dict['id'],
            '_index'     : app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['INDEX_NAME_TEMPLATE'].format(datetime.strptime(input_dict['createdAt'], '%Y-%m-%d %H:%M:%S').year),
            '_type'      : '_doc',
            'url'        : self.gen_article_url(input_dict['forumAlias'], input_dict['id'])
        }

        for key in self.article_es_key_list:
            if key in input_dict:
                dictionary[key] = input_dict[key]

        dictionary['websiteId']      = input_dict['forumId']
        dictionary['website']        = input_dict['forumName']
        dictionary['websiteAlias']   = input_dict['forumAlias']
        dictionary['time']           = input_dict['createdAt']
        dictionary['update_time']    = input_dict['updatedAt']
        dictionary['db_update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        dictionary['reactionCount']  = input_dict['likeCount']

        if dictionary['_index'] not in self.exist_index:
            if not self.es.check_index_exist(dictionary['_index']):
                self.es.create_index(dictionary['_index'], app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['MAPPING_FILEPATH'])
            self.exist_index[dictionary['_index']] = True

        dictionary['reactions'] = {x['id']:x['count'] for x in dictionary['reactions']}
        dictionary['media_data'] = []
        url_dict = {x['url']:None for x in input_dict['media']}
        for mediaMeta_dict in input_dict['mediaMeta']:
            if mediaMeta_dict.get('normalizedUrl') \
            and mediaMeta_dict.get('url') in url_dict:
                dictionary['media_data'].append(
                    {
                        'url' :mediaMeta_dict['url'],
                        'normalizedUrl' :mediaMeta_dict['normalizedUrl']
                    }
                )
        return(dictionary)

    def format_dcard_comment(self, input_dict, year, websiteId, website):
        input_dict = format_datetime_dict(input_dict)
        dictionary = {
            '_id'        : input_dict['id'],
            '_index'     : app.config['ES_SETTING']['ES_INDEX']['COMMENT']['INDEX_NAME_TEMPLATE'].format(year),
            '_type'      : '_doc'
        }
        for key in self.comment_es_key_list:
            if key in input_dict:
                dictionary[key] = input_dict[key]
        dictionary['media_data'] = []
        url_dict = {x['url']:None for x in input_dict['mediaMeta']}
        for mediaMeta_dict in input_dict['mediaMeta']:
            if mediaMeta_dict.get('normalizedUrl') \
            and mediaMeta_dict.get('url') in url_dict:
                dictionary['media_data'].append(
                    {
                        'url' :mediaMeta_dict['url'],
                        'normalizedUrl' :mediaMeta_dict['normalizedUrl']
                    }
                )
        dictionary['key_no'] = input_dict['postId']
        dictionary['reactionCount'] = input_dict.get('likeCount')
        dictionary['time'] = input_dict['createdAt']
        dictionary['update_time'] = input_dict['updatedAt']
        dictionary['db_update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        dictionary['websiteId'] = websiteId
        dictionary['website'] = website
        dictionary['websiteAlias'] = input_dict['websiteAlias']
        return(dictionary)

    ################################################################################
    def dcard_forums_crawler(self, sub_script_name):
        db.session.close()
        # 檢查這台機器是否有同排程還在執行
        if check_duplicate_process(sub_script_name):
            # 代表包含這個程式在內，有兩個以上相同的排程正在運行
            print('{}: 有相同排程尚在執行({})'.format(sub_script_name, 1))
            return

        print(sub_script_name)
        err_code_startw = 0

        forums = self.rdbra.get_forums()
        # 拉不到看板列表，代表該主機被Dcard鎖了，結束排程
        if not forums:
            raise Exception('排程名稱: {}, 訊息: 機器被鎖({}, {})'.format(sub_script_name, err_code_startw, 1))

        print('排程名稱: {}, 訊息: Dcard看板共有 {} 個'.format(sub_script_name, len(forums)))

        forum_id_dict = {forum['id']:forum for forum in forums} 
        db_forum_id_dict = {db_result.id:db_result for db_result in DcardForums.query.all()}
        print('排程名稱: {}, 訊息: DB內 Dcard看板有 {} 個'.format(sub_script_name, len(list(set(db_forum_id_dict.keys())))))
        for forum_id in list(set(list(forum_id_dict.keys()))-set(list(db_forum_id_dict.keys()))):
            forum = forum_id_dict[forum_id]
            forum['pc_l30d'] = forum['postCount']['last30Days']
            forum['backtrack'] = 0
            forum['enable'] = 1
            db_forum_id_dict[forum['id']] = True
            db.session.add(DcardForums(**forum))

        nnn = 0
        for forum_id in list(set(list(db_forum_id_dict.keys()))-set(list(forum_id_dict.keys()))):
            db_result = db_forum_id_dict[forum_id]
            db_result.exist = 0
            db.session.add(db_result)
            nnn+=1
        print('排程名稱: {}, 訊息: 存在於DB內，但Dcard已經關版的看板有 {} 個'.format(sub_script_name, nnn))
        DcardForums.query.filter(DcardForums.ac_time==None).update({'ac_status':0})
        DcardForums.query.filter(DcardForums.cc_time==None).update({'cc_status':0})
        db.session.commit()

    def dcard_article_crawler(self, sub_script_name):
        # 檢查這台機器是否有同排程還在執行
        if check_duplicate_process(sub_script_name):
            # 代表包含這個程式在內，有兩個以上相同的排程正在運行
            print('{}: 有相同排程尚在執行({})'.format(sub_script_name, 1))
            return

        DcardForums.query.filter(
                                DcardForums.ac_status==1
                                ).update(
                                    {
                                        'ac_status':0,
                                        'ac_time':None
                                    }
                                )
        db.session.commit()

        err_code_startw = 1
        try:
            while True:
                db.session.rollback()
                db.session.close()
                DcardForums.query.filter(
                                        DcardForums.ac_status==1,
                                        DcardForums.ac_time!=None,
                                        DcardForums.ac_time<=(datetime.now()-timedelta(hours=6))
                                        ).update({'ac_status':0})
                db.session.commit()
                forum_alias_db_result = DcardForums.query.filter(
                                            DcardForums.ac_status==0,
                                            DcardForums.enable==1,
                                            DcardForums.exist==1,
                                            DcardForums.pc_l30d!=0
                                            ).order_by(
                                                asc(DcardForums.ac_time),
                                                asc(DcardForums.pc_l30d)
                                                ).first()
                # 留言進度未追上文章進度，無文章可爬
                if (forum_alias_db_result.ac_time and forum_alias_db_result.cc_time and forum_alias_db_result.ac_time>forum_alias_db_result.cc_time) \
                or (not forum_alias_db_result.cc_time and forum_alias_db_result.ac_time):
                    print('排程名稱: {}, 訊息: {}'.format(sub_script_name, '留言排程尚未追上文章排程進度，無文章可爬取'))
                    break
                crawler_start_time = datetime.now()
                forum_alias_db_result.ac_time = crawler_start_time
                forum_alias_db_result.ac_status = 1
                db.session.add(forum_alias_db_result)
                db.session.commit()

                forum_alias = forum_alias_db_result.alias
                before_id = None
                finish_status = False
                while not finish_status:
                    if not self.rdbra.request_dcard_status():
                        raise Exception('排程名稱: {}, 訊息: 機器被鎖({}, {})'.format(sub_script_name, err_code_startw, 1))

                    params = pop_dict_empty_value_key(
                        {
                            'before' : before_id,
                            'popular': 'false'
                        }
                    )
                    article_list = self.rdbra.get_article_list(forum_alias, params)
                    if not article_list:
                        print('排程名稱: {}, 無文章'.format(sub_script_name))
                        break
                    batch_load_list = []
                    before_id = '{}'.format(article_list[-1]['id'])

                    print('排程名稱: {}, 工作內容: 爬取看板 {} 七天內文章中, 排程啟動時間: {}, 回溯進度: {}'.format(sub_script_name, forum_alias, crawler_start_time.strftime('%Y-%m-%d %H:%M:%S'), article_list[0]['createdAt']))
                    for article in article_list:
                        tmp_dict = self.rdbra.get_article_content(article['id'])
                        time.sleep(2)
                        if not tmp_dict or not tmp_dict.get('forumAlias'):
                            continue
                        batch_load_dict = self.format_dcard_article(tmp_dict)
                        if not batch_load_dict.get('content') and es.search_by_id(app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['INDEX_NAME'], '_doc', batch_load_dict['_id'])['found']:
                            continue
                        elif datetime.strptime(batch_load_dict['time'], '%Y-%m-%d %H:%M:%S')<=(crawler_start_time-timedelta(days=7)):
                            # 這裡加一層檢查，是為防該看板7天內都沒有任何文章，至少爬一篇最新的進ES，讓檢查看板過去文章是否已匯入這項工作順利執行
                            if not self.es.count({"query":{"bool":{"must":[{"match":{"websiteAlias":forum_alias}}]}}}, app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['INDEX_NAME_TEMPLATE'].format(datetime.strptime(batch_load_dict['time'], '%Y-%m-%d %H:%M:%S').year)) \
                            and not batch_load_list:
                                pass
                            else:
                                print('排程名稱: {}, 看板{}過去七天內文章已爬完'.format(sub_script_name, forum_alias))
                                finish_status = True
                                break
                        batch_load_list.append(batch_load_dict)
                    if batch_load_list:
                        self.batch_load_retryer(batch_load_list)
                forum_alias_db_result.ac_status = 0
                db.session.add(forum_alias_db_result)
                db.session.commit()

            while True:
                db.session.rollback()
                db.session.close()
                crawler_start_time = datetime.now()
                forum_alias_db_result = DcardForums.query.filter(
                                DcardForums.enable==1,
                                DcardForums.backtrack==1,
                                DcardForums.exist==1
                                ).order_by(
                                    asc(DcardForums.ac_time)
                                    ).first()
                if not forum_alias_db_result:
                    print('排程名稱: {}, 訊息: 無需回溯的看板'.format(sub_script_name))
                    break
                forum_alias = forum_alias_db_result.alias
                # 全部看板只給他一小時時間回溯，以免耽擱時間
                if self.crawler_run_over_multi_hours(crawler_start_time, hours=1):
                    print('排程名稱: {}, 訊息: {}'.format(sub_script_name, '回溯執行超過一個小時，停止回溯看板 {}'.format(forum_alias)))
                    break
                # 這邊開始是回溯看板過去文章
                print('排程名稱: {}, 開始檢查看板{}過去文章是否已匯入'.format(sub_script_name, forum_alias))
                query = {
                    "from":0,
                    "size":1,
                    "sort":[
                        {
                            "time":"asc"
                        }
                    ],
                    "query" : {
                        "term":{
                            "websiteAlias":forum_alias
                            }
                    }
                }
                before_id = None
                year_n = 0
                while True:
                    if self.es.check_index_exist(app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['INDEX_NAME_TEMPLATE'].format(crawler_start_time.year-year_n)):
                        tmp_es_result = self.es.search(query, app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['INDEX_NAME_TEMPLATE'].format(crawler_start_time.year-year_n))
                        if tmp_es_result['hits']['hits']:
                            es_result = deepcopy(tmp_es_result)
                    else:
                        break
                    year_n-=1
                if not es_result['hits']['hits']:
                    print('排程名稱: {}, 看板{}無任何文章'.format(sub_script_name, forum_alias))
                    continue
                before_id = '{}'.format(es_result['hits']['hits'][0]['_id'])
                finish_status = False
                while not finish_status:
                    # 全部看板只給他一小時時間回溯，以免耽擱時間
                    if self.crawler_run_over_multi_hours(crawler_start_time, hours=1):
                        print('排程名稱: {}, 訊息: {}'.format(sub_script_name, '回溯執行超過一個小時，停止回溯看板 {}'.format(forum_alias)))
                        break
                    params = {
                        'before' : before_id,
                        'popular': 'false'
                    }
                    for key in list(params.keys()):
                        if not params[key]:
                            params.pop(key)
                    article_list = self.rdbra.get_article_list(forum_alias, params)
                    if not article_list:
                        print('排程名稱: {}, 看板{}已完成回溯所有文章'.format(sub_script_name, forum_alias))
                        break
                    batch_load_list = []
                    before_id = '{}'.format(article_list[-1]['id'])

                    print('排程名稱: {}, 工作內容: 爬取看板 {} 七天內文章中, 排程啟動時間: {}, 回溯進度: {}'.format(sub_script_name, forum_alias, crawler_start_time.strftime('%Y-%m-%d %H:%M:%S'), article_list[0]['createdAt']))
                    for article in article_list:
                        tmp_dict = self.rdbra.get_article_content(article['id'])
                        time.sleep(2)
                        if not tmp_dict or not tmp_dict.get('forumAlias'):
                            continue
                        batch_load_dict = self.format_dcard_article(tmp_dict)
                    if batch_load_list:
                        self.batch_load_retryer(batch_load_list)
                forum_alias_db_result.ac_status = 0
                db.session.add(forum_alias_db_result)
                db.session.commit()
        except Exception as e:
            subject = 'Dcard排程 {} 出現錯誤'.format(sub_script_name)
            message_list = [
                '{}\n'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                '{}\n'.format(str(e)),
                '{}\n'.format(traceback.format_exc())
            ]
            print(traceback.format_exc())
            gs = GmailSender(
                app.config['GOOGLE_SENDER_CONF']['FROM_ADDRESS'],
                app.config['GOOGLE_SENDER_CONF']['RECEIVER_LIST'],
                subject,
                '\n'.join(message_list)
                )
            gs.send_email()

    def dcard_comment_crawler(self, sub_script_name):
        # 檢查這台機器是否有同排程還在執行
        if check_duplicate_process(sub_script_name):
            # 代表包含這個程式在內，有兩個以上相同的排程正在運行
            print('{}: 有相同排程尚在執行({})'.format(sub_script_name, 1))
            return

        DcardForums.query.filter(
                                DcardForums.cc_status==1
                                ).update(
                                    {
                                        'cc_status':0,
                                        'cc_time':None
                                    }
                                )
        db.session.commit()

        err_code_startw = 2
        try:
            while True:
                db.session.rollback()
                db.session.close()
                DcardForums.query.filter(
                                        DcardForums.cc_status==1,
                                        DcardForums.cc_time!=None,
                                        DcardForums.cc_time<=(datetime.now()-timedelta(hours=6))
                                        ).update({'cc_status':0})
                db.session.commit()
                forum_alias_db_result = DcardForums.query.filter(
                                            DcardForums.enable==1,
                                            DcardForums.ac_time!=None,
                                            DcardForums.ac_status==0,
                                            DcardForums.cc_status==0,
                                            DcardForums.exist==1,
                                            DcardForums.pc_l30d!=0
                                            ).order_by(
                                                asc(DcardForums.cc_time),
                                                asc(DcardForums.ac_time),
                                                asc(DcardForums.pc_l30d)
                                                ).first()
                print(forum_alias_db_result.alias)
                if not forum_alias_db_result:
                    print('排程名稱: {}, 訊息: {} ({})'.format(sub_script_name, '文章排程尚未追上留言排程進度，無留言可爬取', 1))
                    break
                elif forum_alias_db_result.cc_time and forum_alias_db_result.ac_time<forum_alias_db_result.cc_time:
                    print('排程名稱: {}, 訊息: {} ({})'.format(sub_script_name, '文章排程尚未追上留言排程進度，無留言可爬取', 2))
                    break

                crawler_start_time = datetime.now()
                forum_alias = forum_alias_db_result.alias

                forum_alias_db_result.cc_time = crawler_start_time
                forum_alias_db_result.cc_status = 1
                db.session.add(forum_alias_db_result)
                db.session.commit()

                article_index_year_list = [forum_alias_db_result.ac_time.year]
                if (forum_alias_db_result.ac_time-timedelta(days=7)).year!=forum_alias_db_result.ac_time.year:
                    article_index_year_list.append(forum_alias_db_result.ac_time.year-1)

                for article_index_year in article_index_year_list:
                    article_index_name = app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['INDEX_NAME_TEMPLATE'].format(article_index_year)
                    comment_index_name = app.config['ES_SETTING']['ES_INDEX']['COMMENT']['INDEX_NAME_TEMPLATE'].format(article_index_year)
                    if article_index_name not in self.exist_index and not self.es.check_index_exist(article_index_name):
                        continue
                    else:
                        self.exist_index[article_index_name] = True
                    if comment_index_name not in self.exist_index:
                        if not self.es.check_index_exist(comment_index_name):
                            self.es.create_index(comment_index_name, app.config['ES_SETTING']['ES_INDEX']['COMMENT']['MAPPING_FILEPATH'])
                        self.exist_index[comment_index_name] = True

                    article_query = {
                        "from":0,
                        "size":100,
                        "sort":[
                            {
                                "time":"asc"
                            }
                        ],
                        "query":{
                            "bool":{
                                "must":[
                                    {
                                        "term":{
                                            "websiteAlias":forum_alias
                                        }
                                    },
                                    {
                                        "range" : {
                                            "commentCount" : {
                                                "gt":0
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                    comment_query = {
                        "from":0,
                        "size":1,
                        "query":{
                            "bool":{
                                "must":[
                                    {
                                        "term":{
                                            "websiteAlias":forum_alias
                                        }
                                    }
                                ]
                            }
                        }
                    }
                    # 如果該看板不是第一次執行留言排程了，就加入時間查詢規則
                    if self.es.search(comment_query, app.config['ES_SETTING']['ES_INDEX']['COMMENT']['INDEX_NAME_TEMPLATE'].format(article_index_year))['hits']['hits']:
                        article_query['query']['bool']['must'].append(
                            {
                                "range" : {
                                    "time" : {
                                        "gte" : (forum_alias_db_result.ac_time-timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S'),
                                        "lte" : forum_alias_db_result.ac_time.strftime('%Y-%m-%d %H:%M:%S')
                                    }
                                }
                            }
                        )
                    while True:
                        es_result = self.es.search(article_query, app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['INDEX_NAME_TEMPLATE'].format(article_index_year))
                        if not es_result['hits']['hits']:
                            break
                        article_query['from']+=article_query['size']
                        print('排程名稱: {}, 工作內容: 爬取看板 {} 七天內文章中, 排程啟動時間: {}, 回溯進度: {}'.format(sub_script_name, forum_alias, crawler_start_time.strftime('%Y-%m-%d %H:%M:%S'), es_result['hits']['hits'][0]['_source']['time']))
                        for article_dict in es_result['hits']['hits']:
                            t1 = time.time()
                            comment_list = self.rdbra.get_article_comments_by_num(article_dict['_id'], input_sleep_time=2)
                            time.sleep(2)
                            if not comment_list:
                                continue
                            batch_load_list = []
                            for comment_dict in comment_list:
                                comment_dict['websiteAlias'] = forum_alias
                                batch_load_dict = self.format_dcard_comment(comment_dict, article_index_year, article_dict['_source']['websiteId'], article_dict['_source']['website'])
                                if not batch_load_dict.get('content') or (not batch_load_dict['content'] and es.search_by_id(comment_index_name, '_doc', batch_load_dict['_id'])['found']):
                                    continue
                                batch_load_list.append(batch_load_dict)
                            if batch_load_list:
                                self.batch_load_retryer(batch_load_list)
                forum_alias_db_result.cc_status = 0
                db.session.add(forum_alias_db_result)
                db.session.commit()

                if forum_alias_db_result.backtrack!=1:
                    continue
                db.session.rollback()
                db.session.close()

                crawler_start_time = datetime.now()
                forum_alias_db_result.cc_time = crawler_start_time
                forum_alias_db_result.cc_status = 1
                db.session.add(forum_alias_db_result)
                db.session.commit()

                # 這邊開始是回溯看板過去文章留言
                print('排程名稱: {}, 開始檢查看板{}過去文章留言是否已匯入'.format(sub_script_name, forum_alias))

                year_n = 0
                article_earliest_time = None
                article_query = {
                    "from":0,
                    "size":1,
                    "sort":[
                        {
                            "time":"asc"
                        }
                    ],
                    "query":{
                        "bool":{
                            "must":[
                                {
                                    "term":{
                                        "websiteAlias":forum_alias
                                    }
                                },
                                {
                                    "range" : {
                                        "commentCount" : {
                                            "gt":0
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
                while True:
                    index_name = app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['INDEX_NAME_TEMPLATE'].format(crawler_start_time.year-year_n)
                    if index_name not in self.exist_index:
                        if not self.es.check_index_exist(index_name):
                            break
                        self.exist_index[index_name] = True
                    es_result = self.es.search(article_query, index_name)
                    if not es_result['hits']['hits']:
                        break
                    article_earliest_time = datetime.strptime(es_result['hits']['hits'][0]['_source']['time'], '%Y-%m-%d %H:%M:%S')
                    year_n+=1

                year_n = 0
                comment_query = {
                    "from":0,
                    "size":1,
                    "sort":[
                        {
                            "postId":"asc"
                        }
                    ],
                    "query":{
                        "bool":{
                            "must":[
                                {
                                    "term":{
                                        "websiteAlias":forum_alias
                                    }
                                }
                            ]
                        }
                    }
                }
                while True:
                    index_name = app.config['ES_SETTING']['ES_INDEX']['COMMENT']['INDEX_NAME_TEMPLATE'].format(crawler_start_time.year-year_n)
                    if index_name not in self.exist_index:
                        if not self.es.check_index_exist(index_name):
                            break
                        self.exist_index[index_name] = True
                    es_result = self.es.search(comment_query, index_name)
                    if not es_result['hits']['hits']:
                        break
                    comment_earliest_article_time = datetime.strptime(self.es.search_by_id(app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['INDEX_NAME_TEMPLATE'].format(crawler_start_time.year-year_n), '_doc', '{}'.format(es_result['hits']['hits'][0]['_source']['postId']))['_source']['time'], '%Y-%m-%d %H:%M:%S')
                    year_n+=1

                if not article_earliest_time:
                    print('排程名稱: {} 訊息: 看板{}沒有已爬入ES的文章，故略過留言回溯'.format(sub_script_name, forum_alias))
                    continue
                article_query = {
                    "from":0,
                    "size":10,
                    "sort":[
                        {
                            "time":"desc"
                        }
                    ],
                    "query":{
                        "bool":{
                            "must":[
                                {
                                    "term":{
                                        "websiteAlias":forum_alias
                                    }
                                },
                                {
                                    "range" : {
                                        "commentCount" : {
                                            "gt":0
                                        }
                                    }
                                },
                                {
                                    "range" : {
                                        "time" : {
                                            "lt":comment_earliest_article_time.strftime('%Y-%m-%d %H:%M:%S')
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
                traceback_stop_status = False
                while comment_earliest_article_time>article_earliest_time:
                    article_index_name = app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['INDEX_NAME_TEMPLATE'].format(comment_earliest_article_time.year)
                    if article_index_name not in self.exist_index:
                        if self.es.check_index_exist(article_index_name):
                            self.exist_index[article_index_name] = True
                        else:
                            break
                    # 每個看板只給他一小時時間回溯，以免耽擱時間
                    elif self.crawler_run_over_multi_hours(crawler_start_time, 1):
                        print('排程名稱: {}, 訊息: {}'.format(sub_script_name, '回溯執行超過一個小時，停止回溯留言 {}'.format(forum_alias)))
                        traceback_stop_status = True
                        break
                    es_result = self.es.search(article_query, app.config['ES_SETTING']['ES_INDEX']['ARTICLE']['INDEX_NAME_TEMPLATE'].format(comment_earliest_article_time.year))
                    if not es_result['hits']['hits']:
                        comment_earliest_article_time = datetime.strptime('{}-12-31 23:59:59'.format(comment_earliest_article_time.year-1), '%Y-%m-%d %H:%M:%S')
                        continue
                    print('排程名稱: {}, 工作內容: 爬取看板 {} 七天內文章中, 排程啟動時間: {}, 回溯進度: {}'.format(sub_script_name, forum_alias, crawler_start_time.strftime('%Y-%m-%d %H:%M:%S'), es_result['hits']['hits'][0]['_source']['time']))
                    for article_dict in es_result['hits']['hits']:
                        comment_list = self.rdbra.get_article_comments_by_num(article_dict['_id'], input_sleep_time=2)
                        time.sleep(2)
                        if not comment_list:
                            continue
                        batch_load_list = []
                        for comment_dict in comment_list:
                            comment_dict['websiteAlias'] = forum_alias
                            batch_load_dict = self.format_dcard_comment(comment_dict, datetime.strptime(article_dict['_source']['time'], '%Y-%m-%d %H:%M:%S').year, article_dict['_source']['websiteId'], article_dict['_source']['website'])
                            if not batch_load_dict.get('content') or (not batch_load_dict['content'] and es.search_by_id(batch_load_dict['_index'], '_doc', batch_load_dict['_id'])['found']):
                                continue
                            batch_load_list.append(batch_load_dict)
                        if batch_load_list:
                            self.batch_load_retryer(batch_load_list)
                    del(article_query['query']['bool']['must'][-1])
                    comment_earliest_article_time = datetime.strptime(es_result['hits']['hits'][-1]['_source']['time'], '%Y-%m-%d %H:%M:%S')
                    article_query['query']['bool']['must'].append({'range':{'time':{'lt':es_result['hits']['hits'][-1]['_source']['time']}}})
                    article_query['from']+=article_query['size']

                forum_alias_db_result.cc_status = 0
                db.session.add(forum_alias_db_result)
                db.session.commit()
                # 全部看板只給一小時回溯留言
                if traceback_stop_status:
                    break
        except Exception as e:
            subject = 'Dcard排程 {} 出現錯誤'.format(sub_script_name)
            message_list = [
                '{}\n'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                '{}\n'.format(str(e)),
                '{}\n'.format(traceback.format_exc())
            ]
            print(traceback.format_exc())
            gs = GmailSender(
                app.config['GOOGLE_SENDER_CONF']['FROM_ADDRESS'],
                app.config['GOOGLE_SENDER_CONF']['RECEIVER_LIST'],
                subject,
                '\n'.join(message_list)
                )
            gs.send_email()