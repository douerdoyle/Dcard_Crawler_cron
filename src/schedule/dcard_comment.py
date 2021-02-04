import sys, os
if __name__ == '__main__':
    sys.path.append('../')
from dcard_crawler import dcard_crawler
if __name__ == '__main__':
    dc = dcard_crawler()
    script_name = os.path.basename(__file__)
    dc.dcard_comment_crawler(script_name)