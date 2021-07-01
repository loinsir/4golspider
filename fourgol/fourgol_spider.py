# -*- coding: utf-8 -*-

import logging
from scrapy.spiders import Spider
import urllib.parse as urlparse


class FourgolSpider(Spider):
    def __init__(self, name=None, **kwargs):
        super().__init__(name=name, **kwargs)
    
    def parse(self, response):
        '''Parsing Thread list or board, request callback is parse_thread'''
        raise NotImplementedError

    def parse_thread(self, response):
        '''Parsing Items in one thread'''
        raise NotImplementedError

    def get_categories(self, category, meta):
        metas = []
        for dict_k, dict_v in category.items():
            category_name = dict_k
            category_code = dict_v

            new_meta = meta.copy()
            new_pc = new_meta['category'][:]
            new_pc.append(category_name)
            new_meta['category'] = new_pc

            depth = new_meta['depth']

            if isinstance(dict_v, dict):
                new_meta['depth'] += 1
                metas.extend(self.get_categories(dict_v, new_meta))
            else:
                new_meta['page'] = self.default_page
                assert self.base_url != None, "Must declare self.base_url in spider."
                new_meta['response_url'] = urlparse.urljoin(self.base_url,category_code)
                metas.append(new_meta)

        return metas
    
    def filter_categories(self, category_dict, categories_to_remove):
        for category_path in categories_to_remove:
            self._delete_category(category_dict, category_path, 1)

    def _delete_category(self, category_dict, paths, depth):

        logger = logging.getLogger(self.name)
        path = paths[depth - 1]

        def filter_func(x):
            if path == '*':
                return True
            return (x == path)

        def nonexists_warn():
            logger.warning(
                'category {} nonexists (path {})'.format(
                    path, ' > '.join(paths)
                )
            )

        if not isinstance(category_dict, dict):
            nonexists_warn()
            return

        found_key = list(filter(filter_func, category_dict.keys()))
        for key in found_key:
            new_paths = paths.copy()
            new_paths[depth - 1] = key
            if depth == len(paths):
                logger.info('removing {}'.format(' > '.join(new_paths)))
                del category_dict[key]
            else:
                self._delete_category(category_dict[key], new_paths, depth + 1)

        if not found_key:
            nonexists_warn()