from furl import furl
import requests

# utility to unshorten url
def unshorten_url(url):
    if len(url) < 30:
        try:
            url = requests.head(url, allow_redirects=True, timeout=5).url
        except:
            pass
    return url

# utility function to strip URLs of the schema, parameters, and www
def strip_url(url):
    url = unshorten_url(url)
    if "youtube.com/watch" in url:
        f = furl(url)
        try:
            v = f.args['v']
            url = f.remove(args=True, fragment=True).url
            url = furl(url).add(args={'v':v}).url
        except:
            pass
    else:
        url = furl(url).remove(args=True, fragment=True).url
    if '://www.' in url:
        url = url.split('://www.',1)[1]
    elif '://' in url:
        url = url.split('://',1)[1]
    return url
