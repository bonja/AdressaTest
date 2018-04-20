
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import re

import pymongo
from pymongo import MongoClient

from bs4 import BeautifulSoup

re_white_spaces = re.compile(r'\s+')
re_new_line = re.compile(r'[\t\n\r]')
re_quote = re.compile(r'[\"\']+')

re_script = re.compile(r'<(script).*?</\1>(?s)')
re_comment = re.compile(r'(<!--.*?-->)')
re_teg = re.compile(r'<[^<>]+>')

re_not_word = re.compile(r'^[\w]+')
re_word = re.compile(r'.*[\w]+.*')
re_helper = re.compile(r'(\.|,|\!|\?|\:|\(|\)){1}')

def clean_string(str_input):
	str_input = re_new_line.sub(' ', str_input)
	str_input = re_white_spaces.sub(' ', str_input)
	str_input = re_quote.sub('', str_input)

	return str_input

def parse_adressa():
	collection = MongoClient('localhost', 27017).adressa.article
	cursor = collection.find()

	count = 0
	for doc in cursor:
		proper = doc.get('proper', None)

		if (proper is not None):
			continue

		url = doc.get('url', None)
		if url is None:
			continue

		html = doc.get('html', None)
		if html is None:
			continue

		html = clean_string(html)
		html = re_comment.sub('', html)
		html = re_script.sub('', html)

		words_header = []
		words_body = []

		soup = BeautifulSoup(html, 'html.parser')
		for key in [('header', 'article-header'), ('body', 'body')]:
			words = []
			for tag in soup.find_all(class_=key[1]):
				if ((key[0] is 'body') and
						('article-body' not in tag.parent['class'])):
					continue
				words = re_not_word.sub(' ', tag.text)
				words = re_helper.sub(' ', words)
				words = re_white_spaces.sub(' ', words)
				words = words.strip().split(' ')

			for word in words:
				if len(word) <= 0:
					continue
				if (not re_word.match(word)):
					continue

				if key[0] is 'header':
				 	words_header.append(word)
				else:
				 	words_body.append(word)

		# This is for wordbase
		words_header = set(words_header)
		words_body = set(words_body)

		# Bad article
		if (len(words_header) == 0) or (len(words_body) < 10):
			collection.update(
				{
					'url': url
				},
				{
					"$set": {
						'proper':False,
					}
				},
				upsert=True
			)
			continue

		collection.update(
			{
				'url': url
			},
			{
				"$set": {
					'words_header': sorted(words_header),
					'words_body': sorted(words_body),
					'proper':True,
				}
			},
			upsert=True
		)

		print len(words_header), len(words_body)

if __name__ == '__main__':
	parse_adressa()

