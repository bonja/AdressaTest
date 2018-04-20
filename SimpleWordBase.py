
import time
import json
import httplib2
import re
import os
import md5
import re

import numpy as np
from pymongo import MongoClient
import pymongo

from bs4 import BeautifulSoup

re_white_spaces = re.compile(r'[\s]+')
re_new_line = re.compile(r'[\t\n\r]')
re_quote = re.compile(r'[\"\']+')
re_script = re.compile(r'<script.+script>')

re_article_header = re.compile(r'article-header')

def clean_string(str_input):
	str_input = re_new_line.sub(' ', str_input)
	str_input = re_white_spaces.sub(' ', str_input)
#	str_input = re_quote.sub('', str_input)
	str_input = re_script.sub('', str_input)

	return str_input

data_set = [
	'/home/darkgs/Dataset/Adressa/three_month/20170301',
	'/home/darkgs/Dataset/Adressa/three_month/20170302',
	'/home/darkgs/Dataset/Adressa/three_month/20170303',
	'/home/darkgs/Dataset/Adressa/three_month/20170304',
]

def generate_article_id(url=None):

	if (url == None):
		print 'Invalid URL for article id'
		return ''

	return md5.md5(url).hexdigest()

def get_article(url=None):

	if not url:
		return None

	article_data = {}

	url_md5 = generate_article_id(url=url)
	article_cache_path = '/home/darkgs/Workspace/AdressaTest/article/' + url_md5

	if os.path.exists(article_cache_path) and os.path.isfile(article_cache_path):
		f = open(article_cache_path, 'r')
		article_data = json.loads(f.read())
		f.close()
	else:
		# This is not a DDOS attack!
		time.sleep(30)
		try:
			response, content = httplib2.Http().request(url)

			soup = BeautifulSoup(content, 'html.parser')

			article_data = {}
			article_data['html'] = soup.get_text()
			article_data['words'] = []

			for p in soup.find_all('p'):
				if p.string == None:
					continue
				article_data['words'] += re.split('[ \.,]', p.string.rstrip())

			f = open(article_cache_path, 'w+')
			f.write(json.dumps(article_data))
			f.close()
		except:
			article_data['html'] = 'None'
			article_data['words'] = []
			return article_data

	return article_data if len(article_data) > 0 else None

def get_most_good_url(event=None):

	if not event:
		return None

	candidate_url = ''
	candidate_url_word_count = 0

	candidates_key = ['url', 'canonicalUrl', 'referrerUrl']

	for key in candidates_key:
		url = event.get(key, None)

		if not url or url.count('/') < 5:
			continue

		# Soft check - Think that longer url is right
		if True:
			if (len(candidate_url) < len(url)):
				candidate_url = url
				candidate_url_word_count = len(get_article(url).get('words', []))
			continue

		# Hard check - get every article to check each word count
		url_data = get_article(url)
		url_data_word_count = len(url_data.get('words', []))

		if url_data_word_count > candidate_url_word_count:
			candidate_url = url
			candidate_url_word_count = url_data_word_count

	if (len(candidate_url) <= 0):
		candidate_url = None
	elif (candidate_url_word_count < 20):
		candidate_url = None

	return candidate_url

def article_crawling_all():
	count = 0
	data_set_dir = '/home/darkgs/Dataset/Adressa/three_month'
	for f_path in os.listdir(data_set_dir):
		f = open(data_set_dir + '/' + f_path, 'r')
		line = f.readline().rstrip()
		while(len(line) >0):
			line = f.readline().rstrip()
			event = json.loads(line)
			user_id = event.get('userId', None)
			url = get_most_good_url(event)

			if (not user_id) or (not url):
				continue

			article_data = get_article(url)

			print str(count) + 'th, from ' + f_path
			count += 1
		f.close()

def article_crawling(seq_count=10):
	a = {}
	u = {}

	sequences = {}
	unique_url = set([])
	unique_user = set([])

	events = []
	word_count_sum = 0

	# Get total events
	for data in data_set:
		data_f = open(data, 'r')
		while(seq_count>0):
			line = data_f.readline()
			events.append(json.loads(line))
			seq_count -= 1
		data_f.close()

		if (seq_count <= 0):
			break

	for event in events:
		sessionStart = event.get('sessionStart', False)
		sessionStop = event.get('sessionStop', False)

		user_id = event.get('userId', None)
		url = get_most_good_url(event)

		if (not user_id) or (not url):
			continue

		article_data = get_article(url)
		word_count_sum += len(article_data.get('words',[]))

#		if (sessionStop):
#			unique_user.add(user_id)

#		if (sessionStart):
#			if user_id in unique_user:
#				print user_id

		unique_url.add(url)

		if sequences.get(user_id, None) == None:
			sequences[user_id] = []
		sequences[user_id].append(url)


	summary = 0.0
	count_more_one = 0
	for value in sequences.values():
		if (len(value) > 0):
			summary += len(value)
			count_more_one += 1

	print 'users : ' + str(len(sequences))
	print 'urls : ' + str(len(unique_url))
	print str(summary/count_more_one) + ' in ' + str(count_more_one)
	print str(word_count_sum) + " words"

	return None

def word_based(seq_count=10):
	u = {}

	word_set = set([])

	# For each event
	seq_iter = seq_count
	for data in data_set:
		data_f = open(data, 'r')
		while(seq_iter>0):
			seq_iter -= 1

			line = data_f.readline()
			event = json.loads(line)

			url = get_most_good_url(event=event)
			article_data = get_article(url=url)

			if article_data == None:
				continue

			word_set.update(article_data.get('words', []))

		data_f.close()

	# This will be a key of embedded a, u
	word_dic = {}
	i = 0
	for word in word_set:
		word_dic[word] = i
		i += 1
	word_set = None
	word_set_size = len(word_dic)
	
	# For each event, Generate User embedding
	seq_iter = seq_count
	count = 0
	for data in data_set:
		data_f = open(data, 'r')
		while(seq_iter>0):
			seq_iter -= 1

			line = data_f.readline()
			event = json.loads(line)

			user_id = event.get('userId', None)

			if user_id == None:
				continue
			
			url = get_most_good_url(event=event)
			article_data = get_article(url=url)

			if article_data == None:
				continue

			count += 1

			if (len(u.get(user_id, [])) == 0):
				u[user_id] = np.zeros(word_set_size)

			for word in article_data.get('words', []):
				u[user_id][word_dic[word]] = 1

	return u, word_dic, count

article_candidate_max = 500
article_candidate_ids = ['' for _ in range(article_candidate_max)]
article_candidate_embeddings = [[] for _ in range(article_candidate_max)]
article_candidate_iter = 0
def article_candidate_add(article_id, article_embedding):
	global article_candidate_max
	global article_candidate_ids
	global article_candidate_embeddings
	global article_candidate_iter

	if article_id in article_candidate_ids:
		return

	article_candidate_ids[article_candidate_iter] = article_id
	article_candidate_embeddings[article_candidate_iter] = article_embedding
	
	article_candidate_iter = (article_candidate_iter + 1) % article_candidate_max

def word_based_predict_for_candidates(article_embedding=[]):
	global article_candidate_max
	global article_candidate_ids
	global article_candidate_embeddings
	global article_candidate_iter

	word_set_size = len(article_embedding)

	max_candidate_id = None
	max_candidate_score = 0.0

	for i in range(article_candidate_max):
		if (len(article_candidate_embeddings[i]) == 0):
			continue

		score = float(np.sum(np.dot(article_candidate_embeddings[i], article_embedding))) / float(word_set_size)
		if (max_candidate_score < score):
			max_candidate_id = article_candidate_ids[i]
			max_candidate_score = score

	return max_candidate_id


def word_based_predict(u={}, word_dic={}, s_seq=100, predicts=10):

	word_set_size = len(word_dic)
	seq_iter = s_seq + predicts

	hit = 0
	total = 0

	for data in data_set:
		data_f = open(data, 'r')
		while(seq_iter>0):
#		while(True):
			seq_iter -= 1

			line = data_f.readline()
			event = json.loads(line)

			# user embedding
			user_id = event.get('userId', None)

			if user_id == None:
				continue

			# article embedding
			url = get_most_good_url(event=event)
			if url ==  None:
				continue
			article_data = get_article(url=url)
			article_id = generate_article_id(url=url)

			if article_id == None or article_data == None:
				continue
				
			a = np.zeros(word_set_size)
			for word in article_data.get('words', []):
				if word not in word_dic.keys():
					continue
				a[word_dic[word]] = 1

			article_candidate_add(article_id, a)

			# Start predict
			if (seq_iter > predicts):
				continue

			# No prev-data
			if (len(u.get(user_id, [])) == 0):
				u[user_id] = np.zeros(word_set_size)
			else:
				if article_id == word_based_predict_for_candidates(article_embedding=u[user_id]):
					hit += 1
				total += 1

			for word in article_data.get('words', []):
				if word not in word_dic.keys():
					continue
				u[user_id][word_dic[word]] = 1
				

	return hit, total

def article_crawling_from_list():
	url_list_file = '/home/darkgs/Workspace/AdressaTest/one_week_url.txt'
	collection = MongoClient('localhost', 27017).adressa.article

	cursor = collection.find()

	url_in_db = []

	for doc in cursor:
		url = doc.get('url', None)
		if url is None:
			continue

		url_in_db.append(url)

	count = 0
	with open(url_list_file, 'r') as f:
		for url in iter(f.readline, ''):
			url = url.rstrip()
		
			if (len(url) <= 0):
				continue

			# 34175 - 30638 = 3,537 filtered
			if re.match('.*(r\.search\.yahoo\.com).*|.*(l\.facebook\.com).*', url):
				continue

			if url in url_in_db:
				continue

			# 30,638
			count += 1

			try:
				response, content = httplib2.Http().request(url)
			except Exception as e:
				with open('/home/darkgs/Workspace/AdressaTest/error.txt', 'a') as err_f:
					err_f.write('HTTP : ' + url + '\n')
					err_f.write(str(e) + '\n')
				continue

			db_entry = {
				'url': url,
				'html': content
			}

			try:
				collection.insert_one(db_entry)
			except Exception as e:
				with open('/home/darkgs/Workspace/AdressaTest/error.txt', 'a') as err_f:
					err_f.write('DB : ' + url + '\n')
#					err_f.write(e + '\n')
				pass
			time.sleep(5)

	f.close()

if __name__ == '__main__':
#	article_crawling(seq_count=10000)
#	u, word_dic, count = word_based(seq_count=15000)
#	hit, total = word_based_predict(u=u, word_dic=word_dic, s_seq=15000, predicts=5000)
#	article_crawling_all()

#	adressa_db = MongoClient('localhost', 27017).adressa
#	collection = adressa_db.article
#	collection.create_index( [("url", pymongo.ASCENDING)], unique=True)

	article_crawling_from_list()

