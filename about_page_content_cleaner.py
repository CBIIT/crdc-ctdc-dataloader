import re
from bento.common.utils import get_logger

logger = get_logger('AboutPageContentCleaner')


class AboutPageContentCleaner:
    @staticmethod
    def clean_text(text):
        # Remove inline links
        cleaned_text = re.sub(r'\$\$\[(.*?)\]\(.*?\)\$\$', r'\1', text)
        # Remove hash tags
        cleaned_text = re.sub(r'\$\$#(.*?)#\$\$', r'\1', cleaned_text)
        # Remove asterisk symbol
        cleaned_text = re.sub(r'\$\$\*(.*?)\*\$\$', r'\1', cleaned_text)
        # Remove extra spaces
        cleaned_text = ' '.join(cleaned_text.split())
        return cleaned_text

    @staticmethod
    def remove_formatting_content(page_name, content):
        cleaned_content = []
        for item in content:
            if isinstance(item, dict) and 'paragraph' in item:
                item['paragraph'] = AboutPageContentCleaner.clean_text(item['paragraph'])
                cleaned_content.append(item)
            # Handling unOrdered List
            elif isinstance(item, dict) and 'listWithDots' in item:
                cleaned_list = []
                for list_item in item['listWithDots']:
                    # handling Alphabets sub orders list
                    if 'listWithAlphabets' in list_item:
                        cleaned_inner_list = [AboutPageContentCleaner.clean_text(inner_list_item) for inner_list_item in list_item['listWithAlphabets']]
                        cleaned_list.append({'listWithAlphabets': cleaned_inner_list})
                    else:
                        cleaned_list.append(AboutPageContentCleaner.clean_text(list_item))
                cleaned_content.append({'listWithDots': cleaned_list})
            elif isinstance(item, dict) and 'listWithAlphabets' in item:
                cleaned_list = [AboutPageContentCleaner.clean_text(list_item) for list_item in item['listWithAlphabets']]
                cleaned_content.append({'listWithAlphabets': cleaned_list})
            # Handling Ordered List with Numbers
            elif isinstance(item, dict) and 'listWithNumbers' in item:
                cleaned_list = []
                for list_item in item['listWithNumbers']:
                    # Handling Alphabets sub orders list
                    if 'listWithAlphabets' in list_item:
                        cleaned_inner_list = [AboutPageContentCleaner.clean_text(inner_list_item) for inner_list_item in list_item['listWithAlphabets']]
                        cleaned_list.append({'listWithAlphabets': cleaned_inner_list})
                    else:
                        cleaned_list.append(AboutPageContentCleaner.clean_text(list_item))
                cleaned_content.append({'listWithNumbers': cleaned_list})
            # Handle table cleaning logic
            elif isinstance(item, dict) and 'table' in item:
                cleaned_table = {
                    'head': [AboutPageContentCleaner.clean_text(cell) for cell in item['table']['head']],
                    'body': [[AboutPageContentCleaner.clean_text(cell) for cell in row] for row in item['table']['body']]
                }
                cleaned_content.append({'table': cleaned_table})
            else:
                cleaned_content.append(item)
        logger.info(f'Cleaned content for "{page_name}"')
        return cleaned_content
