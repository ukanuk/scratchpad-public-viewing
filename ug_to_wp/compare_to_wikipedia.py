"""
Compare values in data.csv with information on Wikipedia.
"""

import datetime
import os
import sys
import time
import json
import mwparserfromhell
import pandas
import requests
from unidecode import unidecode
import wptools


def get_wp_capital(country):
    """Get country Capital from Wikipedia infobox on Country's page.

    Known limitations: Fails if country name initially fetches disambiguation
    page.

    Example usage:
    get_wp_capital('England')

    """

    page = wptools.page(country, silent=True).get_parse()
    infobox = page.data['infobox']
    if infobox is None:
        return None

    try:
        capital = infobox['capital']
    except KeyError:
        return None

    return capital

def get_wikipedia_capital_data(base_data):
    """Get country Capitals from Wikipedia infobox on Country's page.

    Return base_data, but with 'Capital' column values replaced with values
    from country infobox data.

    """

    # Initialize new dataframe for Wikipedia data
    wp_data = pandas.DataFrame(
        index=base_data.index, columns=base_data.columns, dtype=object)

    # From English Wikipedia, get capitals according to country infoboxes
    # Consider only notes with Capital data present (ignore lakes, etc.)
    base_data_notes_with_capitals = base_data.loc[base_data['Capital'].notna()]

    print('\nGetting Wikipedia capitals according to country infoboxes...')
    for ug_country in base_data_notes_with_capitals.index:
        wp_capital = get_wp_capital(ug_country)
        if wp_capital is None:
            print('No capital found in infobox for ' + ug_country)
        else:
            print(ug_country + ': ' + wp_capital)
            wp_data['Capital'][ug_country] = wp_capital

    return wp_data


def get_wp_langs(title, *languages):
    """
    Example usage:
    get_wp_langs('London')
    get_wp_langs('London', 'es', 'af')
    """

    # Be considerate and don't flood requests
    # https://www.mediawiki.org/wiki/API:Etiquette
    time.sleep(1)

    baseurl = 'https://en.wikipedia.org'
    baseapicall = '/w/api.php?action=query&format=json'
    apicallargs = '&prop=langlinks&lllimit=max&titles='
    fullurl = baseurl + baseapicall + apicallargs + title

    page_r = requests.get(fullurl)
    page_json = json.loads(page_r.content)
    pageid = list(page_json['query']['pages'].keys())[0]
    langlinks_json = page_json['query']['pages'][pageid]['langlinks']

    # Convert JSON to dictionary
    langlinks = {}
    for item in langlinks_json:
        langlinks[item['lang']] = item['*']

    if len(languages) == 0:
        return langlinks

    wp_langs = []
    for lang in languages:
        wp_langs.append(langlinks[lang])

    return wp_langs

def get_wikipedia_translation_data(base_data):
    """Get translations from Wikipedia interwiki links.

    Return pandas DataFrame with same indices and columns and base_data, but
    with 'title:languagecode' columns populated with translations from
    interwiki language links based on Wikipedia page title lookups.

    For example with columns 'Country' and 'Country:es', look up the English
    Wikipedia page for 'England' index and get the Spanish (es) translation to
    put in the corresponding 'Country:es' field

    """

    # Initialize new dataframe for Wikipedia data
    wp_data = pandas.DataFrame(
        index=base_data.index, columns=base_data.columns, dtype=object)

    # Read language codes used in anki-ultimate-geography
    ug_langcodes = set()
    for col in base_data.columns:
        pos = col.find(':')
        if pos > 0:
            ug_langcodes.add(col[pos+1::])

    # Create dictionary to map UG language codes to Wikipedia codes
    ug_to_wp_langcodes = {}
    for lang in ug_langcodes:
        # Norwegian (bokmal)
        if lang == 'nb':
            ug_to_wp_langcodes[lang] = 'no'
        else:
            ug_to_wp_langcodes[lang] = lang

    # From Wikipedia, get country/capital translations corresponding
    # to English version of anki-ultimate-geography
    print('Getting Wikipedia translations for country/capital names...')
    for ug_country in base_data.index:

        # Get country name translations
        print('"' + ug_country, end='')
        try:
            translations = get_wp_langs(ug_country)
        except KeyError:
            print('\nNo Wikipedia langlinks found for ' + ug_country)
        else:
            for code in ug_langcodes:
                try:
                    wp_code = ug_to_wp_langcodes[code]
                    this_translation = translations[wp_code]
                except KeyError:
                    print('\nNo Wikipedia translation found for ' +
                          ug_country + ':' + code)
                else:
                    wp_data['Country:'+code][ug_country] = this_translation

        # Get capital name translations
        if pandas.notna(base_data['Capital'][ug_country]):
            print('/' + base_data['Capital'][ug_country], end='')
            try:
                translations = get_wp_langs(base_data['Capital'][ug_country])
            except KeyError:
                print('\nNo Wikipedia langlinks found for ' +
                      ug_country + ':Capital')
            else:
                for code in ug_langcodes:
                    try:
                        wp_code = ug_to_wp_langcodes[code]
                        this_translation = translations[wp_code]
                    except KeyError:
                        print('\nNo Wikipedia translation found for ' +
                              ug_country + ':Capital:' + code)
                    else:
                        wp_data['Capital:'+code][ug_country] = this_translation

        print('"', end=' ', flush=True)

    return wp_data


def one_result_comparison(df1, df2, index, column):
    """Pretty-print index:column values in each table.

    Return string of format 'Index:Col df1_value df2_value', with added
    whitespace to align values

    """

    # Find maximum characters in row, column, and values
    maxchars_index = max(df1.index.str.len())
    maxchars_column = max(df1.columns.str.len())
    df1_maxchars_value = 0
    df2_maxchars_value = 0
    for col in df1.columns:
        length = max(df1[col].str.len())
        if df1_maxchars_value < length:
            df1_maxchars_value = length

        length = max(df2[col].str.len())
        if df2_maxchars_value < length:
            df2_maxchars_value = length

    df1_maxchars_value = int(df1_maxchars_value)
    df2_maxchars_value = int(df2_maxchars_value)

    identifier = '{0:{width}}'.format(
        index + ':' + column, width=maxchars_index + 1 + maxchars_column)
    # Cast to string in case of NaN values, which are right-aligned instead of left-aligned
    ug_identifier = 'UG={0:{width}}'.format(
        str(df1[column][index]), width=df1_maxchars_value)
    wp_identifier = 'WP={0}'.format(
        str(df2[column][index]))

    return identifier + ' ' + ug_identifier + ' ' + wp_identifier


def print_summary(ug_data, wp_data, cmp_data, cmp_data_fuzzy=None, cmp_data_hist=None):
    """Print to console summarizing how ug_data and wp_data compare."""

    vals_total = len(ug_data.index) * len(ug_data.columns)
    vals_nan = 0        # nan vals in orig data, e.g. there's no 'Capital' for 'Antarctica'
    vals_nowp = 0       # nan vals in Wikipedia data where value does exist in UG data
    vals_matching_fuzzy = 0
    vals_mismatch = 0
    vals_mismatch_en = 0
    vals_mismatch_de = 0
    vals_mismatch_es = 0
    vals_mismatch_fr = 0
    vals_mismatch_nb = 0

    print('\n\n\nFields for which UG data exists, but Wikipedia data could not be found:')
    for country in cmp_data.index:
        for col in cmp_data.columns:
            if not cmp_data[col][country]:
                if pandas.isna(wp_data[col][country]):
                    vals_nowp += 1
                    print(country + ':' + col + ' (UG=' +
                          ug_data[col][country] + '), ', end='')
            else:
                if pandas.isna(ug_data[col][country]):
                    vals_nan += 1

    if cmp_data_fuzzy is not None:
        print('\n\n\nFields which probably/mostly match, but should get human',
              'verification for missing accents, incorrect capitalization,',
              'inconsistent abbreviations (St. vs Saint), extra or missing',
              'capitals for countries with more than one, etc.:')
        for country in cmp_data.index:
            for col in cmp_data.columns:
                if cmp_data_fuzzy[col][country] and not cmp_data[col][country]:
                    if pandas.notna(wp_data[col][country]):
                        vals_matching_fuzzy += 1
                        print(one_result_comparison(
                            ug_data, wp_data, country, col))

    print('\n\n\nMismatches beween UG and Wikipedia:')
    for country in cmp_data.index:
        for col in cmp_data.columns:
            if not (cmp_data_fuzzy[col][country] or cmp_data[col][country]):
                if pandas.notna(wp_data[col][country]):
                    vals_mismatch += 1
                    print(one_result_comparison(ug_data, wp_data, country, col))
                    if col.endswith(':de'):
                        vals_mismatch_de += 1
                    elif col.endswith(':es'):
                        vals_mismatch_es += 1
                    elif col.endswith(':fr'):
                        vals_mismatch_fr += 1
                    elif col.endswith(':nb'):
                        vals_mismatch_nb += 1
                    else:
                        vals_mismatch_en += 1

    vals_total_notna = vals_total - vals_nan
    vals_matching = (vals_total_notna
                     - vals_matching_fuzzy
                     - vals_nowp
                     - vals_mismatch)

    print('Total values (incl. UG=NaN): ' + str(vals_total))
    print('  Total values (w/o UG=NaN): ' + str(vals_total_notna))
    print('            Values matching: ' + str(vals_matching))
    print('  Values w/o Wikipedia data: ' + str(vals_nowp))
    print(' Values with fuzzy matching: ' + str(vals_matching_fuzzy))
    print('        Values not matching: ' + str(vals_mismatch))
    print('   Values not matching (en): ' + str(vals_mismatch_en))
    print('   Values not matching (de): ' + str(vals_mismatch_de))
    print('   Values not matching (es): ' + str(vals_mismatch_es))
    print('   Values not matching (fr): ' + str(vals_mismatch_fr))
    print('   Values not matching (nb): ' + str(vals_mismatch_nb))


def merge_data_from_file(filename, base_data=None):
    """
    Return pandas DataFrame populated from a file.
    Optionally, provide base_data as the starting base into which
    file data is imported by matching column and index names
    """
    pass

def compare_data(ug_data, wp_data):
    """Compare two dataframes with exact and fuzzy matching."""

    # Sorting may be required to
    # avoid "ValueError: Can only compare identically-labeled DataFrame objects".
    # Alternately, this error may be caused by a country name change,
    # e.g. from eSwatini to Eswatini
    #ug_data = ug_data.sort_index().sort_index(axis=1)
    #wp_data = wp_data.sort_index().sort_index(axis=1)
    try:
        cmp_data_simple_match = (ug_data == wp_data)
    except ValueError:
        print('')
        print('The loaded ug_data and wp_data do not have identical structure',
              'and cannot be compared')
        print('Try deleting "data_wikipedia_xyz.csv" files to refresh data',
              'from Wikipedia rather than the file cache')
        print('')
        raise

    cmp_data = cmp_data_simple_match.copy()
    cmp_data_fuzzy = cmp_data_simple_match.copy()

    # For values failing simple match, do more advanced comparison
    for country in cmp_data.index:
        for col in cmp_data.columns:

            # Match NaN's
            if pandas.isna(wp_data[col][country]) and pandas.isna(ug_data[col][country]):
                cmp_data[col][country] = True

            # Match exactly, respecting capitals/lowercase and accents
            elif ((not cmp_data[col][country])
                  and pandas.notna(wp_data[col][country])):
                ug_text = ug_data[col][country]
                wp_text_raw = wp_data[col][country]
                wikicode = mwparserfromhell.parse(wp_text_raw)
                wikitext = wikicode.strip_code()
                wp_texts = [
                    wikicode,
                    wikitext,
                    wikitext.strip()]
                wp_suffixes = [
                    ' (country)',
                    ' (ciudad)',
                    ' (pays)',
                    ' (city-state)',
                    ' (Stadt)',
                    ' (stadt)',
                    ' (ville)',
                    ' (by)']
                ug_texts = ([ug_text] +
                            [ug_text + x for x in wp_suffixes])

                # If any item in ug_texts exactly matches any whole string in wp_texts
                if any(s in wp_texts for s in ug_texts):
                    cmp_data[col][country] = True

                # Match approximately, doing anything reasonable to
                # help humans sort through excessive mismatch warnings
                else:
                    wp_texts_fuzzy = (
                        [x.lower() for x in wp_texts] +
                        [x.casefold() for x in wp_texts])
                    wp_texts_fuzzy += [unidecode(x) for x in wp_texts_fuzzy]
                    ug_texts_fuzzy = (
                        [x.lower() for x in ug_texts] +
                        [x.casefold() for x in ug_texts])
                    ug_texts_fuzzy += [unidecode(x) for x in ug_texts_fuzzy]

                    substitutions = {
                        'saint': 'st',
                        '.': '',
                        '-': ' ',
                        '`': '\''}

                    new_fuzzy = []
                    for w in wp_texts_fuzzy:
                        new_w = w
                        for key, value in substitutions.items():
                            if new_w.find(key) > -1:
                                new_w = new_w.replace(key, value)
                        new_fuzzy.append(new_w)
                    wp_texts_fuzzy = new_fuzzy

                    new_fuzzy = []
                    for w in ug_texts_fuzzy:
                        new_w = w
                        for key, value in substitutions.items():
                            if new_w.find(key) > -1:
                                new_w = new_w.replace(key, value)
                        new_fuzzy.append(new_w)
                    ug_texts_fuzzy = new_fuzzy

                    # If any item in ug_texts matches any substring in wp_texts
                    if any(s in w for s in ug_texts_fuzzy for w in wp_texts_fuzzy):
                        cmp_data_fuzzy[col][country] = True

            # Make cmp_fuzzy_data a superset of cmp_data
            if cmp_data[col][country]:
                cmp_data_fuzzy[col][country] = True

    columns_with_unchanged_values = cmp_data.all(axis='index')
    countries_with_unchanged_values = cmp_data.all(axis='columns')

    return cmp_data, cmp_data_fuzzy

def main():
    # os.chdir(r'D:\Documents\GitHub\desktop-tutorial')

    # Import data.csv from GitHub, indexing data on 'Country' column in English
    # Script may have issues if the country name changes
    # TO DO: Check for new page title on Wikipedia page (assuming Wikipedia redirects
    # from page with old country title to page with new country title)
    ug_data_filepath = r'https://github.com/axelboc/anki-ultimate-geography/raw/master/src/data.csv'
    ug_data_full = pandas.read_csv(ug_data_filepath, index_col='Country')

    # Consider only Country and Capital fields along with their translations
    # headers = ug_data_full.columns.to_list()
    # headers_to_ignore = []
    # for i in range(len(headers)-1, -1, -1):
    #     if not (headers[i] == 'Capital'
    #     or headers[i].startswith('Capital:')
    #     or headers[i] == 'Country'
    #     or headers[i].startswith('Country:')):
    #         headers_to_ignore.append( headers.pop(i) )
    headers = [x for x in ug_data_full.columns.to_list()
               if x == 'Capital'
               or x.startswith('Capital:')
               or x == 'Country'
               or x.startswith('Country:')]

    ug_data = ug_data_full[headers]

    # DEBUG: Test on a small subset of data, especially ones which cause exceptions
    # ug_data = ug_data.loc[['Ireland', 'Iceland', 'Georgia', 'South Africa', 'Comoros', 'England', 'Bolivia', 'France', 'Montenegro']]
    # ug_data = ug_data.loc[['South Africa', 'Comoros', 'Bolivia', 'France']]

    # Refreshing Wikipedia data takes a very long time (especially getting Capital data),
    # so typically data should be updated once, saved to file, then future runs
    # of the script should load from the file rather than refreshing from Wikipedia.
    # To force refresh, delete the saved file (if no file is found, update comes from internet)
    wp_data_filename_prefix = 'data_wikipedia_'
    wp_data_filename_suffix = '.csv'
    wp_data_from_file = None  # Initialize so that we can check later whether it loaded
    data_filenames = [x for x in os.listdir()
                      if (x.startswith(wp_data_filename_prefix)
                          and x.endswith(wp_data_filename_suffix))]
    data_filenames.sort()
    try:
        wp_data_filepath = data_filenames[-1]
    except IndexError:
        pass
    else:
        try:
            wp_data_from_file = pandas.read_csv(
                wp_data_filepath, index_col='Country')
            print(wp_data_filepath + " loaded")
        except FileNotFoundError:
            pass

    # In case missing wp_data_from_file, load from web and save for reference
    if isinstance(wp_data_from_file, pandas.core.frame.DataFrame):
        wp_data = wp_data_from_file
    else:
        wp_data = get_wikipedia_translation_data(ug_data)
        wp_data_c = get_wikipedia_capital_data(ug_data)
        wp_data['Capital'] = wp_data_c['Capital']

        date_today = datetime.date.today().strftime('%Y%m%d')
        wp_data_filepath = (wp_data_filename_prefix +
                            date_today + wp_data_filename_suffix)
        wp_data.to_csv(wp_data_filepath, encoding='utf-8', sep=',')

    cmp_data, cmp_data_fuzzy = compare_data(ug_data, wp_data)

    print_summary(ug_data, wp_data, cmp_data, cmp_data_fuzzy)

    # TO DO: Compare to last time Wikipedia results were fetched
    # Get the Wikipedia entries found at last release of anki-ultimate-geography
    # After everything has been manually checked out once, this reduces
    # problem of infobox Capital field containing complex markdown and multiple capitals


if __name__ == "__main__":
    main()
