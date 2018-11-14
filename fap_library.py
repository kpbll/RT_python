import re
import argparse
import json
from datetime import date
import requests
import xmlrpc.client
from bs4 import BeautifulSoup



class ReleaseDate:
    """The object may be created with any of 3 formats of date and allow convert to any of them
    bash = dd-mm, relpage = dd месяц, shedule = mm/dd - день недели двумя буквами"""

    def __init__(self, src_date, year):
        self.src_date = src_date
        self.year = year
        if re.search(r'^\d\d-\d\d$', self.src_date):
            self._format = 'bash'
        elif re.search(r'^\d\d*\s\w*$', self.src_date):
            self._format = 'relpage'
        elif re.search(r'^\d\d*/\d\d*-\w*$', self.src_date):
            self._format = 'schedule'

        self._months_dict = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        self._week_dict = {
            '0': 'пн', '1': 'вт', '2': 'ср', '3': 'чт',
            '4': 'пт', '5': 'сб', '6': 'вс'
        }

    def to_bash(self):
        if self._format == 'bash':
            self.aim_date = self.src_date
        elif self._format == 'relpage':
            day = re.search(r'^(\d\d*).*$', self.src_date).group(1)
            if len(day) == 1:
                day = '0' + day
            month = re.search(r'(\w*)$', self.src_date).group(1)
            month_digit = self._months_dict[month]
            self.aim_date = day + '-' + month_digit
        elif self._format == 'schedule':
            matches = re.search('(\d*)/(\d*)-.*$', self.src_date)
            day = matches.group(2)
            month = matches.group(1)
            self.aim_date = day + '-' + month
        return self.aim_date

    def to_relpage(self):
        if self._format == 'relpage':
            self.aim_date = self.src_date
        elif self._format == 'bash':
            day = re.search(r'^(\d\d*)', self.src_date).group(1)
            month = re.search(r'(\w*)$', self.src_date).group(1)
            month_word = self.month_to_word(month)
            self.aim_date = day + ' ' + month_word
        elif self._format == 'schedule':
            day = re.search(r'^.*/(\d\d)-', self.src_date).group(1)
            month = re.search(r'^(\d\d)/', self.src_date).group(1)
            month_word = self.month_to_word(month)
            self.aim_date = day + ' ' + month_word
        return self.aim_date

    def to_schedule(self):
        if self._format == 'schedule':
            self.aim_date = self.src_date
        elif self._format == 'relpage':
            matches = re.search('(\d*)\s(\w*)', self.src_date)
            day = matches.group(1)
            if len(day) == 1:
                day = '0' + day
            month_word = matches.group(2)
            month_digit = self._months_dict[month_word]
            week_day_digit = date(int(self.year), int(month_digit), int(day)).weekday()
            week_day_word = self._week_dict[str(week_day_digit)]
            self.aim_date = month_digit + '/' + day + '-' + week_day_word
        elif self._format == 'bash':
            day = re.search(r'^(\d\d)-', self.src_date).group(1)
            month = re.search(r'^.*-(\d\d)', self.src_date).group(1)
            week_day_digit = date(int(self.year), int(month), int(day)).weekday()
            week_day_word = self._week_dict[str(week_day_digit)]
            self.aim_date = month + '/' + day + '-' + week_day_word
        return self.aim_date

    def month_to_word(self, month):
        for word, num in self._months_dict.items():
            if num == month:
                return word


class Confluence:
    """Common cases of using confluence api.
    using the set_default_page method for the most cases of using the class will be useful"""

    def __init__(self, url, login, password):
        self.url = url
        self.login = login
        self.password = password
        self.xmlrpc_proxy = xmlrpc.client.ServerProxy(self.url + '/rpc/xmlrpc')
        self.xmlrpc_token = self.xmlrpc_proxy.confluence2.login(self.login, self.password)


class Page:
    def __init__(self, page_title, confluence):
        self.page_title = page_title
        self.confluence = confluence
        self.content = requests.get(
                                    confluence.url +
                                    '/rest/api/content?title=' +
                                    self.page_title +
                                    '&expand=body.storage.value,version.number',
                                    auth=(self.confluence.login, self.confluence.password)
                                    ).json()
        self.content = self.content['results'][0]
        self.page_value = self.content['body']['storage']['value']
        self.content_id = self.content['id']
        self.version = int(self.content['version']['number'])
        self.dict_to_upload = {}

    def _prepare_dict_to_upload(self):
        self.dict_to_upload = {
                              'version': {'number': str(self.version + 1)},
                              'title': self.content['title'],
                              'type': self.content['type'],
                              'body':
                                  {
                                  'storage':
                                           {
                                            'value': self.page_value,
                                            "representation": "storage"
                                           }
                                  }
                              }

    def get_childs(self):
        """returns a list with child id's"""
        get_request = self.confluence.url + '/rest/api/content/search?cql=parent=' + self.content_id
        get_response = requests.get(get_request, auth=(self.confluence.login, self.confluence.password))
        childs_data = get_response.json()['results']
        childs = []
        for child in childs_data:
            #childs.update({child['id']: child['title']})
            childs.append(child['title'])
        return childs

    def set_perms(self, operation, pattern):
        """set permissions for a condluence page by xml pattern"""
        page_perms_response = self.confluence.xmlrpc_proxy.confluence2.setContentPermissions(
                                                                                        self.confluence.xmlrpc_token,
                                                                                        self.content_id,
                                                                                        operation,
                                                                                        pattern
        )
        return page_perms_response

    def update(self):
        """put the current version of content to confluience"""
        self._prepare_dict_to_upload()
        request_data = json.dumps(self.dict_to_upload)
        put_response = requests.put(
                                    self.confluence.url + '/rest/api/content/' + self.content_id,
                                    auth=(self.confluence.login, self.confluence.password),
                                    data=request_data,
                                    headers = {
                                            'Content-Type' : 'application/json',
                                            'Accept' : 'application/json'
                                    }
        )
        return put_response


class Schedule(Page):
    """the class allow updating the release schedule in confluence"""

    def __init__(self, page_title, confluence):
        super().__init__(page_title, confluence)
        self.table = []
        self.product_ru_en_dict = {
            'akeos': 'АКЕОС',
            'armcpok': 'АРМЦПОК',
            'csvc': 'КУ',
            'esia': 'ЕСИА',
            'esnsi(2.0)': 'ЕСНСИ2',
            'esnsi': 'ЕСНСИ',
            'geps': 'ГЕПС',
            'gosbar': 'ГосБар',
            'invest-portal': 'Инвест Портал',
            'ipsh': 'ИПШ',
            'nsmev': 'НСМЭВ',
            'op': 'Открытая Платформа',
            'pgp': 'ПГП',
            'pgu': 'ПГУ',
            'pso': 'ПСО',
            'rc': 'РЦ',
            'rsa': 'РСА',
            'sir': 'СИР',
            'skuf': 'СКУФ',
            'smev': 'СМЭВ',
            'smev-ktda': 'СМЭВ КТДА',
            'ssfo-duus2': 'ССФО-ДУУС 2',
            'amsir': 'Автономные модули',
            'guides': 'Интерактивный Гайд',
            'ifc': 'ИФЦ',
            'ivp': 'ИВП',
            'sedo': 'СЭДО',
        }
        self.changes_counter = 0
        self.add_release = None
        soup = BeautifulSoup(self.page_value, 'html.parser')
        rows = soup.find_all('tr')
        self.top = rows[0]
        rows_iter = iter(rows)
        next(rows_iter)
        for row in rows_iter:
            self.table.append(row.find_all('td'))

    def _update_release_table(self):
        """update data in release table by release pages"""

        for num, row in enumerate(self.table):
            if 'PROD' not in str(row[6]):
                relpage_title = re.search('content-title="(.*)">', str(row[4])).group(1)
                release = Release(relpage_title, self.confluence)
                prod_date_table = re.search('>([\w|\W|\d|\s]*)<', str(row[0])).group(1)
                status_table = re.search('<strong>([\w|\W]*)</strong>', str(row[6])).group(1)
                date_prod_moved_table = re.search('>([\w|\W|\d|\s]*)<', str(row[1])).group(1)
                finalize_date_table = re.search('>([\w|\W|\d|\s]*)<', str(row[2])).group(1)
                prod_date_page = ReleaseDate(release.date_prod, release.year).to_schedule()
                if release.date_prod_moved is not '':
                    prod_date_moved_page = ReleaseDate(release.date_prod_moved, release.year).to_schedule()
                else:
                    prod_date_moved_page = release.date_prod_moved
                finalize_date_page = ReleaseDate(release.date_finalize, release.year).to_schedule()
                if prod_date_table.count(prod_date_page) == 0 \
                        or status_table != release.status\
                        or finalize_date_table.count(finalize_date_page) == 0\
                        or date_prod_moved_table.count(prod_date_moved_page) == 0:
                    self.changes_counter = self.changes_counter + 1
                # build rows in table with parsed data
                    row[0] = '<td colspan="1"><span>' + prod_date_page + '</span></td>'
                    row[1] = '<td colspan="1"><span>' + prod_date_moved_page + '</span></td>'
                    row[2] = '<td colspan="1"><span>' + finalize_date_page + '</span></td>'
                    row[6] = '<td colspan="1"><strong>' + release.status + '</strong></td>'

    def add_release_to_schedule(self, release):
        """add the row to schedule table(need to updating schedule to upload)"""
        release = Release(release, self.confluence)
        self.add_release = release.release_page
        if self.add_release in str(self.table):
            print('WARN: the release already added to the release schedule')
            self.add_release = None
            return False
        new_row_pattern = '<td colspan="1">пн, 10-сен</td>, <td colspan="1">пн, 10-сен</td>, <td colspan="1">' \
                          'пн, 10-сен</td>, <td colspan="1">ПГУ</td>, <td colspan="1"><ac:link>' \
                  '<ri:page ri:content-title="PGU-Release-3.0.229.1-rpguservices"></ri:page><ac:plain-text-link-body>' \
                  '<![CDATA[3.0.229.1-rpguservices]]></ac:plain-text-link-body></ac:link></td>, <td colspan="1">' \
                  'Плановый</td>, <td colspan="1"><strong>PROD</strong></td>, <td colspan="1"><br/></td>, ' \
                  '<td colspan="1"><br/></td>'
        soup = BeautifulSoup(new_row_pattern, 'html.parser')
        new_line = soup.find_all('td')
        release = Release(release.release_page, self.confluence)
        date_finalize = ReleaseDate(release.date_finalize, release.year).to_schedule()
        date_prod = ReleaseDate(release.date_prod, release.year).to_schedule()
        date_prod_moved = ""
        try:
            _product_ru = self.product_ru_en_dict[release.product.lower()]
        except:
            _product_ru = 'UNKNOWN PRODUCT'
        new_line[0] = '<td colspan="1">' + date_prod + '</td>'
        if release.date_prod_moved is not None:
            date_prod_moved = ReleaseDate(release.date_prod_moved, release.year).to_schedule()
            new_line[1] = '<td colspan="1">' + date_prod_moved + '</td>'
        else:
            new_line[1] = '<td colspan="1"></td>'
        new_line[2] = '<td colspan="1">' + date_finalize + '</td>'
        new_line[3] = '<td colspan="1">' + _product_ru + '</td>'
        new_line[4] = '<td colspan="1"><ac:link><ri:page ri:content-title="' + \
                      release.release_page + \
                      '"></ri:page><ac:plain-text-link-body><![CDATA[' + \
                      release.release_ver + \
                      ']]></ac:plain-text-link-body></ac:link></td>'
        new_line[5] = '<td colspan="1">' + release.type + '</td>'
        new_line[6] = '<td colspan="1"><strong>' + release.status + '</strong></td>'
        self.table.append(new_line)


    def update_schedule_page(self):
        """compile results to dict and put to confluence"""

        self._update_release_table()
        if self.changes_counter is 0 and self.add_release is None:
            print('OK: No changes found on release pages. No need to update the Schedule.')
            exit(0)
        before_table = re.search('(.*<tbody>)<tr>', self.page_value).groups(1)[0]
        after_table = '</tbody></table></ac:rich-text-body></ac:structured-macro>'
        table = ''
        # generate dict-json to put to confluence
        for row in self.table:
            columns = ''
            for column in row:
                columns = columns + (str(column))
            table = table + '<tr>' + columns + '</tr>'
        self.page_value = before_table + str(self.top) + table + after_table
        put_response = self.update()
        self.add_release = None
        return put_response


class Release:
    """object contents changable parsed elements from release page and allow change them"""
    def __init__(self, relpage, confluence):
        self.release_page = relpage
        self.relpage = Page(relpage, confluence)
        self.status = re.search(
                                'Статус:[&nbsp;\s]*<strong>([\w|\W|\d|\s]*?)</strong></li>',
                                self.relpage.page_value
                                ).group(1)
        self.year = self.relpage.content['_expandable']['space'][-4:]
        self.product = re.search('([\w\W\d\D\s]*)-[r|R]elease', self.release_page).group(1)
        self.release_ver = re.search('[R|r]elease[s]*-([\w\W\d\D\s]*)', self.release_page).group(1)
        self.type = re.search(
                                r'Тип релиза:[&nbsp;\s]*[<strong>]*(\w*?)<[/strong></li></ul><ul><li>Установка]*',
                                self.relpage.page_value
                             ).group(1)
        self.date_prod_moved = ''
        self.date_finalize_moved = ''
        if re.search('<li>Финализация.*<s>.*енесено.*тест', self.relpage.page_value):
            self.date_finalize = re.search(
                                        '<li>Финализация.*еренесено.*(\d\d\s\w*).*</li><li>.*Завершение',
                                        self.relpage.page_value
                                        ).group(1)
            self.date_finalize_moved = True
        else:
            self.date_finalize = re.search(
                                            '<li>Финализация.*(\d\d\s\w*).*</li><li>.*Завершение',
                                            self.relpage.page_value
                                        ).group(1)

        if re.search('<li>.*продуктив.*<s>.*енесено.*инали', self.relpage.page_value):
            matches = re.search(
                                '<li>.*продуктив.*<s>.*(\d\d\s\w*).*</s>.*еренесено на.*(\d\d\s\w*).*</li><li>.*Финал',
                                self.relpage.page_value
                                )
            self.date_prod = matches.group(1)
            self.date_prod_moved = matches.group(2)
        else:
            self.date_prod = re.search('<li>.*продуктив.*(\d\d\s\w*).*</li><li>.*Финал', self.relpage.page_value).group(1)


    def move_date_prod(self, date):
        new_date = ReleaseDate(date, self.year).to_relpage()
        if self.date_prod_moved:
            src_regexp = r'перенесено на: \d\d\s\w*</li><li>Финализация'
            aim_regexp = r'перенесено на: ' + new_date + '</li><li>Финализация'
            self.relpage.page_value = re.sub(src_regexp,
                                             aim_regexp,
                                             self.relpage.page_value)
        else:
            src_regexp = r'(.*Установка[&nbsp;\s]*в[&nbsp;\s]*продуктив[&nbsp;\s]*-[&nbsp;\s]*)([\d|\s|\w|\s]*)</li>'
            aim_regexp =  r'\1' + '<s>' + r'\2' + '</s>' + ' перенесено на: ' + new_date + '</li>'
            self.relpage.page_value = re.sub(src_regexp,
                                            aim_regexp,
                                            self.relpage.page_value)

    def move_date_finzlize(self, date):
        new_date = ReleaseDate(date, self.year).to_relpage()
        if self.date_finalize_moved:
            src_regexp = r'перенесено на: \d\d\s\w*</li><li>Завершение'
            aim_regexp = r'перенесено на: ' + new_date + '</li><li>Завершение'
            self.relpage.page_value = re.sub(src_regexp,
                                             aim_regexp,
                                             self.relpage.page_value)
        else:
            src_regexp = r'(.*Финализация[&nbsp;\s]*релиза[&nbsp;\s]*-[&nbsp;\s]*)([\d|\s|\w|\s]*)</li>'
            aim_regexp = r'\1' + '<s>' + r'\2' + '</s>' + ' перенесено на: ' + new_date + '</li>'
            self.relpage.page_value = re.sub(src_regexp,
                                             aim_regexp,
                                             self.relpage.page_value)

    def set_status(self, new_status):
        self.status = new_status
        src_regexp = r'(.*<li>Статус: <strong>).+(</strong></li><li>Тип релиза.*)'
        aim_regexp = r'\1' + new_status + r'\2'
        self.relpage.page_value = re.sub(src_regexp,
                                        aim_regexp,
                                        self.relpage.page_value)

def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True, help='confluence url, example: https://confluence.egovdev.ru')
    parser.add_argument('--login', required=True, help='your login in confluence')
    parser.add_argument('--password', required=True, help='your password in confluence')
    parser.add_argument('--page_title', help='example: SSFO-DUUS2-Release-2.2.2.2, required for:\
                                                                    --set_status_finalized, --set_permissions')
    parser.add_argument('--set_status_finalized', action='store_true',
                        help='change a status on a release page to finalized. ')
    parser.add_argument('--set_permissions', action='store_true', help='set restrictions for release pages')
    parser.add_argument('--update_schedule', help='update the release schedule page, schedule page title required')
    parser.add_argument('--add_release_to_schedule', help='add a row to schedule, required release page title')
    parser.add_argument('--move_prod_date',
                        help='change date of installing to prod on release page, value: dd-mm')
    parser.add_argument('--move_finalize_date',
                        help='change date of finalizing on release page, value: dd-mm')
    return parser


if __name__ == "__main__":
    parser = create_parser()
    args_namespace = parser.parse_args()
    confluence = Confluence(args_namespace.url, args_namespace.login, args_namespace.password)
    if args_namespace.set_status_finalized is True:
        try:
            release = Release(args_namespace.page_title, confluence)
            release.set_status('Финализирован')
            resp = release.relpage.update()
            if '200' in str(resp):
                print('OK: Release status was changed.')
            else:
                print('FAILED: Release status was not changed.')
        except IndexError:
            print('FAILED: release page does not exist.')
        except Exception as e:
            print('FAILED: something went wrong with status updating.', e)
    if args_namespace.set_permissions is True:
        try:
            release = Release(args_namespace.page_title, confluence)
            to_set_perms_list = release.relpage.get_childs()
            to_set_perms_list.append(release.relpage.page_title)
            for page in to_set_perms_list:
                page = Page(page, confluence)
                resp = page.set_perms(
                                'Edit',
                                [{"groupName": "confluence-contentmgn"}]
                )
                if resp is True:
                    print('OK: Permissions for the page', page.page_title, 'was set.')
                else:
                    print('FAILED: Permissions for the page', page.page_title, 'was not set.')
        except Exception as e:
            print('FAILED: Something went wrong with permissions setting.', e)
    if args_namespace.move_finalize_date is not None:
        try:
            release = Release(args_namespace.page_title, confluence)
            release.move_date_finzlize(args_namespace.move_finalize_date)
            resp = release.relpage.update()
            if '200' in str(resp):
                print('OK: finalizing date was moved.')
            else:
                print('FAILED: finalizing date was not moved. HTTP Error:', resp.content)
        except Exception as e:
            print('FAILED: Something went wrong moving the date.', e)
    if args_namespace.move_prod_date is not None:
        try:
            release = Release(args_namespace.page_title, confluence)
            release.move_date_prod(args_namespace.move_prod_date)
            resp = release.relpage.update()
            if '200' in str(resp):
                print('OK: Installing to prod date was moved.')
            else:
                print('FAILED: Installing to prod date was not moved. HTTP Error:', resp.content)
        except Exception as e:
            print('FAILED: Something went wrong moving the date.', e)
    # SCHEDULE UPDATING SHOULD BE IN THE END CAUSE OF THAT CONTENTS EXIT()
    if args_namespace.update_schedule is not None:
        try:
            schedule = Schedule(args_namespace.update_schedule, confluence)
            if args_namespace.add_release_to_schedule is not None:
                schedule.add_release_to_schedule(args_namespace.add_release_to_schedule)
            resp = schedule.update_schedule_page()
            if '200' in str(resp):
                print('OK: Schedule updated successfully.')
            else:
                print('FAILED: Schedule was not updated correctly. HTTP Error:', resp.content)
        except Exception as e:
             print('FAILED: Something went wrong with schedule updating.', e)

