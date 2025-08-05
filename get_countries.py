import requests
import csv
import time
from typing import Dict, List, Tuple, Optional
import json
import langcodes


def get_countries_with_iso_codes() -> List[Dict[str, str]]:
    """
    Query Wikidata SPARQL endpoint to get all countries with 2-letter ISO codes
    Returns list of dicts with 'qid' and 'iso_code' keys
    """
    sparql_query = """
    SELECT DISTINCT ?country ?countryLabel ?iso_code WHERE {
      ?country wdt:P31/wdt:P279* wd:Q6256.  # instance of country
      ?country wdt:P297 ?iso_code.           # ISO 3166-1 alpha-2 code
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    ORDER BY ?iso_code
    """

    url = "https://query.wikidata.org/sparql"
    headers = {
        'User-Agent': 'WikidataCountryLabelsExtractor/1.0 (https://example.com/contact)',
        'Accept': 'application/json'
    }
    params = {
        'query': sparql_query,
        'format': 'json'
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        countries = []
        seen_qids = set()  # To avoid duplicates

        for item in data['results']['bindings']:
            qid = item['country']['value'].split('/')[-1]  # Extract Q-ID from URI
            iso_code = item['iso_code']['value']

            if qid not in seen_qids:
                countries.append({
                    'qid': qid,
                    'iso_code': iso_code
                })
                seen_qids.add(qid)

        return countries
    except Exception as e:
        print(f"Error fetching countries: {e}")
        return []


def convert_to_iso639_1(wikidata_lang_code: str) -> Optional[str]:
    """
    Convert Wikidata language code to ISO 639-1 (2-letter) code
    Returns None if no valid ISO 639-1 code exists
    """
    try:
        # Handle special cases and variations
        # Wikidata uses some non-standard codes
        special_mappings = {
            'simple': 'en',  # Simple English
            'zh-hans': 'zh',  # Simplified Chinese
            'zh-hant': 'zh',  # Traditional Chinese
            'nb': 'no',  # Norwegian Bokmål -> Norwegian
            'nds': 'de',  # Low German -> German (no ISO 639-1 for nds)
            'nds-nl': 'nl',  # Low Saxon (Netherlands) -> Dutch
            'be-tarask': 'be',  # Belarusian (Taraškievica)
            'sr-ec': 'sr',  # Serbian (Cyrillic)
            'sr-el': 'sr',  # Serbian (Latin)
        }

        if wikidata_lang_code in special_mappings:
            return special_mappings[wikidata_lang_code]

        # Try to parse with langcodes
        lang = langcodes.get(wikidata_lang_code)

        # Get the ISO 639-1 code if it exists
        if lang and lang.language:
            # Try to get the two-letter code
            if len(lang.language) == 2:
                return lang.language
            # Try to find ISO 639-1 equivalent
            try:
                standardized = langcodes.standardize_tag(lang.language)
                if standardized and len(standardized.split('-')[0]) == 2:
                    return standardized.split('-')[0]
            except:
                pass

        # If direct parsing fails, try just the language part (before hyphen)
        if '-' in wikidata_lang_code:
            base_lang = wikidata_lang_code.split('-')[0]
            if len(base_lang) == 2:
                return base_lang
            # Try to parse the base language
            try:
                lang = langcodes.get(base_lang)
                if lang and lang.language and len(lang.language) == 2:
                    return lang.language
            except:
                pass

        # If it's already a 2-letter code, return it
        if len(wikidata_lang_code) == 2:
            return wikidata_lang_code

        return None
    except:
        return None


def get_all_entity_labels(qid: str) -> Dict[str, List[str]]:
    """
    Fetch ALL labels and aliases for a given Wikidata entity
    Returns dict with structure: {wikidata_lang_code: [all_labels_and_aliases]}
    """
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    headers = {
        'User-Agent': 'WikidataCountryLabelsExtractor/1.0 (https://example.com/contact)'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        entity_data = data['entities'][qid]
        result = {}

        # Extract ALL labels
        if 'labels' in entity_data:
            for lang, label_data in entity_data['labels'].items():
                if lang not in result:
                    result[lang] = []
                result[lang].append(label_data['value'])

        # Extract ALL aliases
        if 'aliases' in entity_data:
            for lang, aliases_list in entity_data['aliases'].items():
                if lang not in result:
                    result[lang] = []
                for alias_data in aliases_list:
                    result[lang].append(alias_data['value'])

        # Remove duplicates while preserving order
        for lang in result:
            seen = set()
            result[lang] = [x for x in result[lang] if not (x in seen or seen.add(x))]

        return result
    except Exception as e:
        print(f"Error fetching labels for {qid}: {e}")
        return {}


def extract_country_labels_to_csv(output_file: str = 'country_labels.csv',
                                  delay_between_requests: float = 0.5,
                                  include_wikidata_lang_code: bool = True) -> None:
    """
    Main function to extract all country labels and save to CSV

    Args:
        output_file: Path to output CSV file
        delay_between_requests: Delay in seconds between API requests (be respectful!)
        include_wikidata_lang_code: If True, includes original Wikidata language code in CSV
    """
    print("Fetching countries with ISO codes...")
    countries = get_countries_with_iso_codes()
    print(f"Found {len(countries)} countries with ISO codes")

    # Statistics
    total_labels = 0
    languages_found = set()
    conversion_failures = set()

    # Prepare CSV file
    fieldnames = ['label', 'iso639_1_language', 'qid', 'country_iso_code']
    if include_wikidata_lang_code:
        fieldnames.insert(2, 'wikidata_language')

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Process each country
        for i, country in enumerate(countries):
            qid = country['qid']
            iso_code = country['iso_code']

            print(f"Processing {i + 1}/{len(countries)}: {qid} ({iso_code})...")

            # Get all labels and aliases
            labels_by_lang = get_all_entity_labels(qid)

            # Write to CSV
            for wikidata_lang, labels_list in labels_by_lang.items():
                # Convert to ISO 639-1
                iso639_1_code = convert_to_iso639_1(wikidata_lang)

                if iso639_1_code is None:
                    conversion_failures.add(wikidata_lang)
                    # Skip entries without valid ISO 639-1 codes
                    # Uncomment next line to include them with empty ISO 639-1 field
                    # iso639_1_code = ''
                    continue

                languages_found.add(iso639_1_code)

                # Write each unique label
                for label in labels_list:
                    row = {
                        'label': label,
                        'iso639_1_language': iso639_1_code,
                        'qid': qid,
                        'country_iso_code': iso_code
                    }
                    if include_wikidata_lang_code:
                        row['wikidata_language'] = wikidata_lang

                    writer.writerow(row)
                    total_labels += 1

            # Be respectful to the API
            time.sleep(delay_between_requests)

    print(f"\nExtraction complete!")
    print(f"Total labels extracted: {total_labels}")
    print(f"Unique ISO 639-1 languages: {len(languages_found)}")
    print(f"Data saved to: {output_file}")

    if conversion_failures:
        print(f"\nWikidata language codes without ISO 639-1 mapping ({len(conversion_failures)}):")
        for lang in sorted(conversion_failures)[:10]:
            print(f"  - {lang}")
        if len(conversion_failures) > 10:
            print(f"  ... and {len(conversion_failures) - 10} more")


def get_country_labels_sample(limit: int = 5) -> None:
    """
    Get a sample of country labels for testing (limited number of countries)
    Shows all labels and language code conversions
    """
    print(f"Getting sample data for {limit} countries...")
    countries = get_countries_with_iso_codes()[:limit]

    for country in countries:
        qid = country['qid']
        iso_code = country['iso_code']
        print(f"\n{'=' * 60}")
        print(f"Country: {qid} (ISO: {iso_code})")
        print(f"{'=' * 60}")

        labels_by_lang = get_all_entity_labels(qid)

        # Show sample of labels with conversions
        sample_langs = sorted(labels_by_lang.keys())[:5]
        for wikidata_lang in sample_langs:
            iso639_1 = convert_to_iso639_1(wikidata_lang)
            print(f"\n  Wikidata code: {wikidata_lang} → ISO 639-1: {iso639_1}")
            print(f"  Labels ({len(labels_by_lang[wikidata_lang])}):")
            for label in labels_by_lang[wikidata_lang]:
                print(f"    - {label}")

        if len(labels_by_lang) > 5:
            print(f"\n  ... and {len(labels_by_lang) - 5} more languages")


def analyze_language_coverage(csv_file: str = 'country_labels.csv') -> None:
    """
    Analyze the extracted data to show language coverage statistics
    """
    print(f"\nAnalyzing {csv_file}...")

    lang_counts = {}
    country_counts = {}
    total_rows = 0

    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            lang = row['iso639_1_language']
            country = row['country_iso_code']

            lang_counts[lang] = lang_counts.get(lang, 0) + 1
            country_counts[country] = country_counts.get(country, 0) + 1

    print(f"\nTotal entries: {total_rows}")
    print(f"Countries: {len(country_counts)}")
    print(f"Languages: {len(lang_counts)}")

    print("\nTop 10 languages by number of labels:")
    for lang, count in sorted(lang_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {lang}: {count} labels")

    print("\nTop 10 countries by number of labels:")
    for country, count in sorted(country_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {country}: {count} labels")


# Example usage
if __name__ == "__main__":
    # Test with a small sample first
    if False:
        print("Testing with sample data...")
        get_country_labels_sample(limit=3)

    if True:
        print("\nStarting full extraction...")
        extract_country_labels_to_csv('data/wikidata_country_labels.csv')

        analyze_language_coverage('data/wikidata_country_labels.csv')