#!/usr/bin/env python2

USAGE = """\
This script allows manual uploading of cover images from Bandcamp to
MusicBrainz.

Usage: %(cmd)s bandcamp_url [mbid ...]
MBIDs can be given as musicbrainz.org URLs, will be automatically parsed.
Example: %(cmd)s http://clearsignals.bandcamp.com/album/stars-lost-your-name e672cf29-6b11-4819-aad0-5748cbe8452e
"""

import sys
import os
import re
import mechanize

from editing import MusicBrainzClient
import utils
import config as cfg

try:
    from PIL import Image

except ImportError:
    Image = None
    print "Warning: Cannot import PIL. Install python-imaging for image dimension information"

try:
    import psycopg2
    from psycopg2.extras import NamedTupleCursor
except ImportError:
    psycopg2 = None

BC_CACHE = 'bc-cache'

utils.monkeypatch_mechanize()

def re_find1(regexp, string):
    m = re.findall(regexp, string)
    if len(m) != 1:
        pat = getattr(regexp, 'pattern', regexp)
        if len(string) > 200:
            filename = '/tmp/debug.html'
            with open(filename, 'wb') as f:
                f.write(string)
            raise AssertionError("Expression %s matched %d times, see %s" % (pat, len(m), filename))
        else:
            raise AssertionError("Expression %s matched %d times: %r" % (pat, len(m), string))
    return m[0]

def create_parent_dir(filename):
    dirname = os.path.dirname(filename)
    if not os.path.exists(dirname):
        print "Creating directory %r" % dirname
        os.mkdir(dirname)

# Progress file - prevent duplicate uploads
DBFILE = os.path.join(BC_CACHE, 'progress.db')
try:
    statefile = open(DBFILE, 'r+')
    state = set(x.strip() for x in statefile.readlines())
except IOError: # Not found? Try writing
    create_parent_dir(DBFILE)
    statefile = open(DBFILE, 'w')
    state = set()

def done(line):
    assert line not in state
    statefile.write("%s\n" % line)
    statefile.flush()
    state.add(line)

#### DOWNLOADING

# <a class="popupImage" href="http://f0.bcbits.com/img/a1101927415_10.jpg">
bc_image_rec = re.compile('<a class="popupImage" href="(.+\.(jpg|png|gif))">')
# http://clearsignals.bandcamp.com/album/stars-lost-your-name
bc_url_rec = re.compile('https?://([^/]+?)(?:\.bandcamp\.com)?/album/([^/]+)')

def download_cover(img_url, filename):
    if os.path.exists(filename):
        print "SKIP download, already done: %r" % filename
        return

    print "Downloading %r to %r" % (img_url, filename)

    resp = br.open_novisit(img_url)
    data = resp.read()
    resp.close()

    create_parent_dir(filename)
    with open(filename, 'wb') as f:
        f.write(data)

def fetch_cover(url):
    host, album = re_find1(bc_url_rec, url)

    resp = br.open(url)
    data = resp.read()

    title = br.title()
    referrer = br.geturl()
    print "Title: %s" % title

    img_url, ext = re_find1(bc_image_rec, data)
    dest_filename = os.path.join(BC_CACHE, host, '%s.%s' % (album, ext))

    del data

    download_cover(img_url, dest_filename)

    cover = {
        'referrer': referrer,
        'file': dest_filename,
        'title': title,
    }
    return cover

#### IMAGE PROCESSING

def pretty_size(size):
    # http://www.dzone.com/snippets/filesize-nice-units
    suffixes = [('',2**10), ('k',2**20), ('M',2**30), ('G',2**40), ('T',2**50)]
    for suf, lim in suffixes:
        if size > lim:
            continue
        else:
            return "%s %sB" % (round(size/float(lim/2**10),1), suf)

def annotate_image(filename):
    """Returns image information as dict"""
    data = {}
    data['size_bytes'] = bytesize = os.stat(filename).st_size
    data['size_pretty'] = pretty_size(bytesize)

    if Image:
        img = Image.open(filename)
        try:
            # Verify image - makes sure we don't upload corrupt junk
            img.tostring()

        except IOError as err:
            print "Error in image %r: %s" % (filename, err)
            sys.exit(1)
        data['dims'] = img.size
    else:
        data['dims'] = None

    return data

#### UPLOADING

COMMENT = ""
def upload_cover(cov, mbid):
    upload_id = "%s %s" % (mbid, cov['referrer'])

    if upload_id in state:
        print "SKIP upload, already done: %r" % cov['file']
        return

    types = ['front']
    note  = "\"%(title)s\"\nfrom %(referrer)s\n" % cov
    note += "Size: %(size_pretty)s (%(size_bytes)s bytes)" % cov
    if cov['dims']:
        note += " / Dimensions: %dx%d" % cov['dims']

    print "Uploading %(file)r (%(size_pretty)s)" % cov

    mb.add_cover_art(mbid, cov['file'], types, None, COMMENT, note, False)

    done(upload_id)

#### CORE

def handle_bc_cover(bc_url, mbids):
    print "Downloading from", bc_url
    cover = fetch_cover(bc_url)

    if mbids:
        data = annotate_image(cover['file'])
        cover.update(data)
        init_mb()

    for mbid in mbids:
        mburl = '%s/release/%s/cover-art' % (cfg.MB_SITE, mbid)
        print "Uploading to", mburl
        upload_cover(cover, mbid)
        print "Done!", mburl

def print_help():
    print USAGE % {'cmd': sys.argv[0]}

uuid_rec = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
def bot_main():
    if len(sys.argv) <= 1 or '--help' in sys.argv or '-h' in sys.argv:
        print_help()
        sys.exit(1)

    bc_url = None
    mbids = []
    for arg in sys.argv[1:]:
        if uuid_rec.findall(arg):
            mbids.append(re_find1(uuid_rec, arg))

        elif bc_url_rec.findall(arg):
            if bc_url is not None:
                print "Specify only one bandcamp URL"
                sys.exit(1)
            bc_url = arg

        else:
            print "Unrecognized argument:", arg
            print
            print_help()
            sys.exit(1)

    init_br()
    if bc_url:
        handle_bc_cover(bc_url, mbids)

def init_br():
    global br

    br = mechanize.Browser()
    br.set_handle_robots(False) # no robots
    br.set_handle_refresh(False) # can sometimes hang without this
    br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]

def init_mb():
    global mb

    print "Logging in..."
    mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

if __name__ == '__main__':
    bot_main()
