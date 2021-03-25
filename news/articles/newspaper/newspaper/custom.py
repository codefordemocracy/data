from urllib.parse import urlparse, urljoin as urljoin2

def parse_proxy_url(url, **kwargs):
    if url is None:
        return None
    proxy_url = ''
    proxy_string = ''
    source_url = url
    if '&url=' in url:
        proxy_url = url.split('&url=',1)[0]+'&url='
        source_url = url.split('&url=',1)[1]
        proxy_string = proxy_url.split('://',1)[1]+urlparse(source_url, **kwargs).scheme+'://'
    source_domain = urlparse(source_url, **kwargs).netloc
    return [proxy_string+source_domain, source_domain, proxy_url, source_url]

def urljoin(source_url, url):
    proxy_url =  parse_proxy_url(source_url)[2]
    source_url = parse_proxy_url(source_url)[3]
    url = parse_proxy_url(url)[3]
    return proxy_url + urljoin2(source_url, url)
