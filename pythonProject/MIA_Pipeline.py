# Minneapolis Institute of Art Data Pipeline Project
# Laura Hengel


import pandas as pd
import json
import glob
import re
from datetime import date
import numpy as np
import logging

TODAY = date.today()  # this is going to be used to filter out dates that may be in the future

err_logger = logging.getLogger('error')
err_handler = logging.FileHandler('error.log')
err_logger.addHandler(err_handler)
err_logger.setLevel(logging.ERROR)
chg_logger = logging.getLogger('change')
changelog_handler = logging.FileHandler('change.log')
format_change = logging.Formatter('[%(asctime)s] -- %(message)s')
changelog_handler.setFormatter(format_change)
chg_logger.addHandler(changelog_handler)
chg_logger.setLevel(logging.INFO)


# clean departments
def clean_depts(path):
    # get department data from given path
    dept_df = []
    for file in glob.glob(path):
        dept_df.append(pd.read_json(file))
    departments = pd.concat(dept_df)
    departments.drop_duplicates(subset=['artworks'], inplace=True)
    # Check existing dept data to see if we need to make any changes or if we are good to go
    current_dept = pd.read_csv('C:\\Users\\henge\\PycharmProjects\\MIA\\final_tables\\departments.csv')
    current_dept.drop_duplicates(subset=['artworks'], inplace=True)
    upload_departments = departments[~np.isin(departments.artworks.unique(), current_dept.artworks.unique())]
    return upload_departments


# clean artworks
def make_inches_num(in_string):
    just_nums = re.sub(r'[^0-9\-/\s]*', '', in_string)
    string_parts = just_nums.strip().replace('-', ' ').split(' ')
    decimal = 0.0
    if len(string_parts) > 3:
        # add message here about wrong format (warning)
        return float('Nan')  # this will be my flag to grab entries that I think have messed up size information.
    else:
        for part in string_parts:
            if re.search(r'[^0-9/]', part) or part == '':
                # add message here about measurement being in wrong format (warning)
                return float('Nan')
            elif re.search('/', part):
                numerator, denominator = part.split('/')
                addition = float(numerator) / int(denominator)
            else:
                addition = float(part)
            decimal += addition
    return decimal


def era_convert(*args, century):
    eras = []
    for item in args:
        if 'BCE' in item or 'BC' in item:
            eras.append(-1)
        elif 'CE' in item or 'AD' in item:
            eras.append(1)
        else:
            eras.append(0)
    subs = [0, 0]
    if century != 1:
        if eras == [1, 1]:
            subs = [-1, 0]
        elif eras == [-1, -1]:
            subs = [0, 1]
    else:
        subs = [0, 0]
    return eras, subs


# I think the best foot forward is to make a function that can then be passed through using apply
def deconstruct_dated(indate):
    str_date = str(indate)
    dates = []  # begin splitting the current string
    multiply = 1
    if not re.search(r'[0-9]', str_date) or str_date == '':
        return [float('Nan'), float('Nan')]
    else:
        if re.search('century', str_date, re.I) is not None:  # converting century to an actual number
            multiply = 100
        str_date = str_date.replace('–', '-')

        if '-' in str_date:
            dates = str_date.split('-')
            times = re.findall(r'BCE|CE|BC|AD', str_date)
        else:
            dates.append(str_date)
            times = re.findall(r'BCE|CE|BC|AD', str_date)
        if len(times) == 0:  # account for no specification of BCE or CE
            times = ['CE', 'CE']

        if len(times) != len(dates):  # accounts for entries where there is only one specification of a BCE/CE
            times.append(times[0])

        times, subs = era_convert(*times, century=multiply)

        # now let's go through and edit these a bit more
        # sub default is [0,1] if century present
        today = date.today()
        # print(type(today.year))
        d_converted = []
        for t, d, s in zip(times, dates, subs):
            d_clean = int(re.sub(r'[^0-9]*', '', d))
            # put a check here for ridiculous numbers
            if (d_clean * t) > today.year:
                d_clean = int(d_clean / 10000)
            d_converted.append(((d_clean * t) + s) * multiply)

        if len(d_converted) == 1:
            d_converted.append(d_converted[0])

        if d_converted[0] > d_converted[1]:
            d_converted[1] = d_converted[1] + (int(d_converted[0] / 100) * 100)

        return d_converted


def ages_decode(number):
    age = ''
    if number <= -3000:
        age = 'stone age'
    elif number <= -1200:
        age = 'bronze age'
    elif number <= -800:
        age = 'iron age'
    elif number <= 476:
        age = 'classical age'
    elif number <= 1450:
        age = 'middle ages'
    else:
        age = 'modern age'
    return age


def clean_artworks(path):
    artwork_df = []
    for file in glob.glob(path):
        try:
            artwork_df.append(pd.read_json(file))
        except ValueError as e:
            err_logger.error('{} is not formatted consistently'.format(file))
    artworks = pd.concat(artwork_df).drop_duplicates(subset=['accession_number'])
    core_artwork = artworks.drop(
        columns=['art_champions_text', 'catalogue_raissonne', 'culture', 'description', 'image', 'image_copyright',
                 'image_height', 'image_width', 'inscription', 'life_date', 'markings', 'nationality', 'portfolio',
                 'provenance', 'restricted', 'rights_type', 'role', 'see_also', 'signed', 'text', 'title',
                 'object_name'])
    # add important columns for the id number and whether work is displayed or not
    core_artwork['id'] = core_artwork.id.apply(lambda x: int(x[(x.rindex('/')+1):]))
    core_artwork['display'] = core_artwork.room.apply(lambda x: 0 if x == 'Not on View' else 1)
    # fix dimensions
    # take out any entries that contain no dimension
    no_dimensions = core_artwork[core_artwork['dimension'].isnull()]
    core_artwork.dropna(subset='dimension', inplace=True)
    core_artwork['dimension'] = core_artwork.dimension.apply(
        lambda x: '0 in' if not re.search('[0-9]', x) else (x + 'in' if not re.search('in', x) else x))
    core_artwork['dimension'] = core_artwork.dimension.apply(lambda x: x[:x.index('in')])
    core_artwork['dimension'] = core_artwork.dimension.apply(lambda z: z.replace('×', 'x'))
    expanded_dimensions = core_artwork['dimension'].str.split('x', expand=True)
    expanded_dimensions.rename({0: 'height', 1: 'width', 2: 'depth'}, axis='columns', inplace=True)
    expanded_dimensions.replace({None: '0'}, inplace=True)
    expanded_dimensions['height'] = expanded_dimensions.height.apply(make_inches_num)
    expanded_dimensions['width'] = expanded_dimensions.width.apply(make_inches_num)
    expanded_dimensions['depth'] = expanded_dimensions.depth.apply(make_inches_num)
    if len(expanded_dimensions.columns) > 3:
        expanded_dimensions.drop(labels=3, axis=1, inplace=True)
    core2_artwork = pd.concat([core_artwork, expanded_dimensions], axis=1)
    # take out any entries that show they have a null dimension (the format was incorrect)
    wrong_dimensions = core2_artwork[core2_artwork[['height', 'width', 'depth']].isna().any(axis=1)]
    core2_artwork.dropna(axis=0, subset=['height', 'width', 'depth'], inplace=True)
    # normalize the 'dated' column
    holder = core2_artwork['dated'].apply(deconstruct_dated)
    expanded_date = pd.DataFrame()
    expanded_date['start'] = holder.apply(lambda x: x[0])
    expanded_date['end'] = holder.apply(lambda x: x[1])
    artwork_complete = pd.concat([core2_artwork, expanded_date], axis=1)
    missing_dates = artwork_complete[artwork_complete[['start', 'end']].isna().any(axis=1)]
    artwork_complete.dropna(axis=0, subset=['start', 'end'], inplace=True)
    artwork_complete['age'] = artwork_complete.start.apply(ages_decode)
    artwork_complete.drop(columns=['dated', 'department', 'dimension', 'room'], axis=1, inplace=True)
    artwork_complete['continent'] = artwork_complete.continent.fillna('Unknown')
    artwork_complete['country'] = artwork_complete.country.fillna('Unknown')

    current_art = pd.read_csv('C:\\Users\\henge\\PycharmProjects\\MIA\\final_tables\\artworks.csv')
    upload_artworks = artwork_complete[~np.isin(artwork_complete.id.unique(), current_art.id.unique())]
    return upload_artworks


# clean exhibitions - this one should make two tables (exhibits, and exhibit-artwork table)
def format_dates(date):
    formatted = ''
    date = date.replace(' to', '')
    try:
        formatted = pd.to_datetime(date, format='mixed')
    except:
        formatted = np.datetime64('NaT')
    else:
        if date == '':
            formatted = np.datetime64('NaT')
    return formatted


def clean_exhibits(path):
    exhibit_df = []
    for file in glob.glob(path):
        info = 0
        try:
            with open(file, 'r') as file:
                info = json.load(file)
        except Exception as error:
            chg_logger.info('file is empty')
            continue

        for art in info['objects']:
            one_line = {'exhibition_id': info['exhibition_id'], 'art_id': art, 'display_date': info['display_date']}
            exhibit_df.append(one_line)

    exhibits = pd.DataFrame(exhibit_df)
    exhibit_art_df = exhibits[['exhibition_id', 'art_id']]
    exhibit_art_df.drop_duplicates(inplace=True)

    current_exhibit_art = pd.read_csv('C:\\Users\\henge\\PycharmProjects\\MIA\\final_tables\\exhibit_art.csv')
    upload_exhibit_art = exhibit_art_df[~np.isin(exhibit_art_df, current_exhibit_art)]

    exhibits = exhibits.drop_duplicates(subset=['exhibition_id'])
    exhibits.drop('art_id', inplace=True, axis=1)
    expanded_show_dates = pd.DataFrame()
    expanded_show_dates[['start', 'end']] = exhibits['display_date'].str.split(r'-| to', expand=True)
    expanded_show_dates['end'] = expanded_show_dates['end'].fillna('')
    expanded_show_dates['start_datetime'] = expanded_show_dates.start.apply(format_dates)
    expanded_show_dates['end_datetime'] = expanded_show_dates.end.apply(format_dates)

    final_exhibits = pd.concat([exhibits, expanded_show_dates], axis=1)
    try:
        final_exhibits['days'] = final_exhibits['end_datetime'] - final_exhibits['start_datetime']
    except:
        err_logger.error('Error with calculation of number of days exhibit was on')
    incorrect_format_dates = final_exhibits[final_exhibits['days'].isnull()]
    final_exhibits.dropna(subset=['days'], axis=0, how='any', inplace=True)
    final_exhibits['days'] = final_exhibits['days'].dt.days

    current_exhibit = pd.read_csv('C:\\Users\\henge\\PycharmProjects\\MIA\\final_tables\\exhibits.csv')
    upload_exhibit = final_exhibits[~np.isin(final_exhibits.exhibition_id.unique(), current_exhibit.exhibition_id.unique())]
    return upload_exhibit_art, upload_exhibit



def main():
    new = clean_depts('C:\\Users\\henge\\PycharmProjects\\MIA\\collection-main\\departments\\*.json')
    if len(new) > 0:
        print('oh weird')
    else:
        print('yeah that is right')

    test2 = clean_artworks('C:\\Users\\henge\\PycharmProjects\\MIA\\collection-main\\objects\\1\\*.json')
    if len(test2) > 0:
        print('uh oh')
        print(len(test2))
    else:
        print('right again')

    test3, test4 = clean_exhibits('C:\\Users\\henge\\PycharmProjects\\MIA\\collection-main\\exhibitions\\0\\*.json')
    if len(test3) >0:
        print('oh darn')
    else:
        print('oh yeah')

    if len(test4) > 0:
        print('no why')
    else:
        print('woohoo')




main()