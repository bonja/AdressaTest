
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

re_word = re.compile(r'^[\w]+')
re_helper = re.compile(r'(\.|,|-|\!|\?){1}')

def clean_string(str_input):
	str_input = re_new_line.sub(' ', str_input)
	str_input = re_white_spaces.sub(' ', str_input)
	str_input = re_quote.sub('', str_input)

	return str_input

def parse_adressa():
	collection = MongoClient('localhost', 27017).adressa.article
	cursor = collection.find(
		{
			'url' : 'http://adressa.no/kultur/article57961.ece'
		}
	)
	cursor = collection.find()

	count = 0
	for doc in cursor:
		html = doc.get('html', None)
		if html is None:
			continue

		html = clean_string(html)
		html = re_comment.sub('', html)
		html = re_script.sub('', html)

		words_header = []
		words_body = []

		soup = BeautifulSoup(html, 'html.parser')
		for key in [('header', 'article-header'), ('body', 'article-body')]:
			words = []
			for tag in soup.find_all(class_=key[1]):
				words = re_word.sub(' ', tag.text)
				words = re_helper.sub(' ', words)
				words = re_white_spaces.sub(' ', words)
				words = words.strip().split(' ')
			if key[0] is 'header':
			 	words_header += words
			else:
			 	words_body += words

		# Bad article
		if (len(words_header) == 0) or (len(words_body) < 10):
			print len(words_header), len(words_body)

if __name__ == '__main__':
	parse_adressa()
