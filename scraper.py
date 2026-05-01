import json
import os
import uuid
import re
import sys
from datetime import datetime, timedelta


def load_json(filename):
    try:
        with open(filename) as f:
            return json.load(f)
    except:
        return []


def save_json(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)


def parse_price(price_raw):
    if isinstance(price_raw, (int, float)):
        return int(price_raw)
    price_str = str(price_raw).replace('$', '').replace(',', '').replace('S', '').replace('RM', '').replace(' ', '')
    try:
        return int(float(price_str))
    except:
        return 0


def parse_psf(psf_raw):
    if isinstance(psf_raw, (int, float)):
        return float(psf_raw)
    match = re.search(r'[\d,]+\.\d+|[\d,]+', str(psf_raw).replace(',', ''))
    try:
        return float(match.group(0)) if match else 0
    except:
        return 0


def parse_size(size_raw):
    if isinstance(size_raw, (int, float)):
        return int(size_raw)
    match = re.search(r'(\d+)', str(size_raw))
    return int(match.group(1)) if match else 800


def run_scrape():
    token = os.environ.get('APIFY_TOKEN', '')
    if not token:
        print("ERROR: APIFY_TOKEN not set", file=sys.stderr)
        return False

    import requests
    
    # Configurable via env vars
    data_dir = os.environ.get('DATA_DIR', 'data')
    property_type = os.environ.get('PROPERTY_TYPE', 'condo')
    search_url = os.environ.get('SEARCH_URL', '')
    
    if not search_url:
        # Default condo search
        search_url = (
            "https://www.propertyguru.com.sg/property-for-sale?"
            "_freetextDisplay=Ang+Mo+Kio%2C+Bishan%2C+Bukit+Merah%2C+Bukit+Timah%2C+Downtown+Core%2C+Hougang%2C+Kallang%2C+Katong%2C+Kovan%2C+Marine+Parade%2C+Museum%2C+Newton%2C+Novena%2C+Orchard%2C+Outram%2C+Queenstown%2C+River+Valley%2C+Robertson+Quay%2C+Rochor%2C+Sentosa%2C+Serangoon%2C+Singapore+River%2C+Tanglin%2C+Tiong+Bahru%2C+Toa+Payoh"
            "&bathrooms=2&bedrooms=2&bedrooms=3&maxPrice=1320000&maxPricePerArea=1700&minPricePerArea=1100"
            "&minSize=730&minTopYear=2006&order=asc&propertyTypeCode=APT&propertyTypeCode=CLUS"
            "&propertyTypeCode=CONDO&propertyTypeCode=EXCON&propertyTypeCode=WALK&propertyTypeGroup=N"
            "&sort=psf&subZoneIds=41007&subZoneIds=41015&subZoneIds=41031&subZoneIds=41072&subZoneIds=41162"
            "&zoneIds=40004&zoneIds=40006&zoneIds=40011&zoneIds=40017&zoneIds=40021&zoneIds=40022"
            "&zoneIds=40025&zoneIds=40029&zoneIds=40034&zoneIds=40035&zoneIds=40036&zoneIds=40039"
            "&zoneIds=40040&zoneIds=40043&zoneIds=40044&zoneIds=40045&zoneIds=40046&zoneIds=40048"
            "&zoneIds=40051&zoneIds=40052"
        )

    # Singapore actor - supports condo, hdb, landed
    actor = {
        "name": "abotapi/propertyguru-sg-scraper",
        "endpoint": "abotapi~propertyguru-sg-scraper",
        "input": {
            "mode": "url",
            "urls": [search_url],
            "listing_type": "sale",
            "property_type": property_type,
            "max_properties": 0,
            "max_pages": 20
        }
    }

    url = f"https://api.apify.com/v2/acts/{actor['endpoint']}/run-sync-get-dataset-items"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    print(f"\nRunning Singapore actor: {actor['name']}")
    print(f"Property type: {property_type}")
    print(f"Data dir: {data_dir}")
    
    raw_listings = []
    
    try:
        resp = requests.post(url, json=actor['input'], headers=headers, timeout=600)
        print(f"Status: {resp.status_code}")
        
        if resp.status_code in [200, 201]:
            try:
                data = resp.json()
                if isinstance(data, list):
                    raw_listings = data
                elif isinstance(data, dict) and 'data' in data:
                    raw_listings = data['data']
                print(f"Got {len(raw_listings)} listings")
                
                if len(raw_listings) > 0:
                    print(f"First listing: {json.dumps(raw_listings[0])[:300]}")
            except Exception as e:
                print(f"Parse error: {e}")
        else:
            print(f"Error {resp.status_code}: {resp.text[:500]}")
            
    except Exception as e:
        print(f"Request error: {e}")
    
    if not raw_listings:
        print(f"\nWARNING: No {property_type} listings from Apify. Keeping previous data.")
        return True

    print(f"\nProcessing {len(raw_listings)} {property_type} listings...")
    
    today = datetime.now().strftime('%Y-%m-%d')
    listings = []
    properties_map = {}

    for idx, raw in enumerate(raw_listings):
        try:
            pg_id = raw.get('listing_id', raw.get('external_id', str(idx)))
            l_id = f"pg-{pg_id}"
            
            price_val = parse_price(raw.get('price', raw.get('price_value', 0)))
            if price_val == 0:
                continue
            
            size_sqft = parse_size(raw.get('size', ''))
            psf = parse_psf(raw.get('price_per_area', 0))
            if psf == 0 and price_val and size_sqft:
                psf = round(price_val / size_sqft, 2)
            
            beds = 2
            baths = 2
            bed_match = re.search(r'(\d+)', str(raw.get('bedrooms', '')))
            bath_match = re.search(r'(\d+)', str(raw.get('bathrooms', '')))
            if bed_match:
                beds = int(bed_match.group(1))
            if bath_match:
                baths = int(bath_match.group(1))
            
            prop_name = raw.get('project_name', raw.get('title', 'Unknown'))
            if ' at ' in prop_name:
                prop_name = prop_name.split(' at ')[0].strip()
            address = raw.get('location', raw.get('address', ''))
            
            prop_key = f"{prop_name}|{address}|{beds}|{baths}|{size_sqft}"
            
            if prop_key in properties_map:
                prop_id = properties_map[prop_key]['id']
                is_dup = True
                dup_of = properties_map[prop_key]['first_listing']
                properties_map[prop_key]['listing_ids'].append(l_id)
                properties_map[prop_key]['total_listings'] += 1
            else:
                prop_id = f"prop-{uuid.uuid4().hex[:8]}"
                is_dup = False
                dup_of = None
                properties_map[prop_key] = {
                    'id': prop_id,
                    'name': prop_name,
                    'address': address,
                    'property_type': raw.get('property_type', property_type),
                    'bedrooms': beds,
                    'bathrooms': baths,
                    'size_sqft': size_sqft,
                    'top_year': raw.get('build_year'),
                    'tenure': raw.get('tenure', ''),
                    'first_seen': today,
                    'last_seen': today,
                    'listing_ids': [l_id],
                    'current_price': price_val,
                    'current_psf': psf,
                    'status': 'active',
                    'price_changes': 0,
                    'total_listings': 1,
                    'first_listing': l_id,
                }
            
            listing = {
                'id': l_id,
                'pg_listing_id': str(pg_id),
                'pg_url': raw.get('url', raw.get('listing_url', '')),
                'property_id': prop_id,
                'property_name': prop_name,
                'address': address,
                'price': price_val,
                'psf': psf,
                'size_sqft': size_sqft,
                'bedrooms': beds,
                'bathrooms': baths,
                'property_type': raw.get('property_type', property_type),
                'status': 'active',
                'agent_name': raw.get('agent_name', ''),
                'listing_date': today,
                'last_seen': today,
                'days_on_market': 0,
                'description': raw.get('description', '')[:500],
                'is_duplicate': is_dup,
                'duplicate_of': dup_of,
                'top_year': raw.get('build_year'),
                'tenure': raw.get('tenure', ''),
                'fetched_at': datetime.now().isoformat(),
            }
            listings.append(listing)
            
        except Exception as e:
            print(f"Error parsing listing {idx}: {e}")
            continue

    existing_listings = load_json(f'{data_dir}/listings.json')
    existing_price_history = load_json(f'{data_dir}/price_history.json')
    existing_snapshots = load_json(f'{data_dir}/snapshots.json')
    existing_weekly = load_json(f'{data_dir}/weekly_highlights.json')
    
    scraped_ids = {l['id'] for l in listings}
    for el in existing_listings:
        if el['id'] not in scraped_ids and el.get('status') == 'active':
            el['status'] = 'inactive'
            el['last_seen'] = today
    
    existing_ids = {l['id'] for l in existing_listings}
    new_listings_only = []
    
    for l in listings:
        if l['id'] in existing_ids:
            for idx, el in enumerate(existing_listings):
                if el['id'] == l['id']:
                    if el['price'] != l['price'] and el['price'] > 0:
                        change_amt = l['price'] - el['price']
                        change_pct = round(change_amt / el['price'] * 100, 1)
                        existing_price_history.append({
                            'id': f"ph-{uuid.uuid4().hex[:8]}",
                            'listing_id': l['id'],
                            'property_id': l['property_id'],
                            'property_name': l['property_name'],
                            'price': l['price'],
                            'psf': l['psf'],
                            'change_amount': change_amt,
                            'change_percent': change_pct,
                            'recorded_at': today,
                            'notes': f"Price changed from S${el['price']:,}"
                        })
                        for pk, prop in properties_map.items():
                            if prop['id'] == l['property_id']:
                                prop['price_changes'] += 1
                                break
                    existing_listings[idx] = l
                    break
        else:
            existing_listings.append(l)
            new_listings_only.append(l)
    
    properties = list(properties_map.values())
    existing_props = load_json(f'{data_dir}/properties.json')
    existing_prop_ids = {p['id'] for p in properties}
    for ep in existing_props:
        if ep['id'] not in existing_prop_ids:
            ep['status'] = 'inactive'
            properties.append(ep)
    
    active = [l for l in existing_listings if l.get('status') == 'active']
    snapshot = {
        'id': f"snap-{uuid.uuid4().hex[:8]}",
        'week_start': (datetime.strptime(today, '%Y-%m-%d') - timedelta(days=6)).strftime('%Y-%m-%d'),
        'week_end': today,
        'total_listings': len(existing_listings),
        'active_listings': len(active),
        'inactive_listings': len(existing_listings) - len(active),
        'new_listings': len(new_listings_only),
        'went_inactive': sum(1 for l in existing_listings if l.get('status') == 'inactive' and l.get('last_seen') == today),
        'price_changes': len([ph for ph in existing_price_history if ph.get('recorded_at') == today]),
        'avg_price': round(sum(l['price'] for l in active) / len(active)) if active else 0,
        'avg_psf': round(sum(l['psf'] for l in active) / len(active), 2) if active else 0,
        'created_at': datetime.now().isoformat(),
    }
    existing_snapshots.append(snapshot)
    
    newly_inactive = [l for l in existing_listings if l.get('status') == 'inactive' and l.get('last_seen') == today]
    price_changes_today = [ph for ph in existing_price_history if ph.get('recorded_at') == today]
    
    if new_listings_only or newly_inactive or price_changes_today:
        existing_weekly.append({
            'week_start': snapshot['week_start'],
            'week_end': today,
            'new_listings': [{'listing_id': l['id'], 'property_name': l['property_name'], 'address': l['address'], 'price': l['price']} for l in new_listings_only],
            'went_inactive': [{'listing_id': l['id'], 'property_name': l['property_name'], 'address': l['address'], 'last_price': l['price']} for l in newly_inactive],
            'price_changes': [{'listing_id': ph['listing_id'], 'property_name': ph['property_name'], 'address': '', 'old_price': ph['price'] - ph['change_amount'], 'new_price': ph['price'], 'change_amount': ph['change_amount'], 'change_percent': ph['change_percent']} for ph in price_changes_today],
        })
    
    os.makedirs(data_dir, exist_ok=True)
    save_json(f'{data_dir}/listings.json', existing_listings)
    save_json(f'{data_dir}/properties.json', properties)
    save_json(f'{data_dir}/price_history.json', existing_price_history)
    save_json(f'{data_dir}/snapshots.json', existing_snapshots)
    save_json(f'{data_dir}/weekly_highlights.json', existing_weekly)
    
    print(f"\nSAVED to {data_dir}: {len(existing_listings)} listings, {len(properties)} properties")
    print(f"SNAPSHOT: {snapshot['active_listings']} active, {snapshot['new_listings']} new")
    return True


if __name__ == '__main__':
    success = run_scrape()
    sys.exit(0)
