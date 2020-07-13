# Fast multithreaded Scraper for phpBB forums

 * Multi-process and multi-threaded Scraper for phpBB forums.
 * Utilizing the same HTTP(S) session to minimize opened connections and TLS-handshakes.
 * Reconstructs BB codes from HTML.
 * Saves output as JSON files under forum/subforum directory structure.
 * Has options to save attached medias (files & images) as well as saving external images ([img] and [fimg] codes).
 * Supports authorization through User-Agent and Cookie headers.

## Prerequisits

 * Python3.x

### Requirements

```
$ pip3 install orjson BeautifulSoup
```

## Usage

```
$ phpbb-scraper.py [options] URL

Options:

  -f, --forum FORUM-SPEC
    Specify FORUM to scrape. Could be specified multiple times.
    FORUM-SPEC might contains several IDs as range or list:

      $ phpbb-scraper.py -f 2,5-10 ...

  -t, --topic TOPIC-SPEC
    Specify Topics. Could be specified multiple times.
    TOPIC-SPEC might contains several IDs as range or list:

      $ phpbb-scraper.py -t 8-20,40,30,1-5,3 ...

  -p, --pool-size SIZE
    Set pool size to SIZE

  -w, --workers WORKERS
    Set number of Process Workers to WORKERS

  -a, --user-agent USER-AGENT
  -c, --cookie COOKIE
    Authorize using USER-AGENT and COOKIE strings

  -o, --output DIRECTORY
    Scrape forum under DIRECTORY

  --force
    Force overwrite operation for topics even if they are already scraped

  -m, --save-media
    Save external media files ([img] and [fimg] links)

  -s, --save-attachments
    Save posts' attachments

  -u, --save-users
    Dump users list

  --parse-date
    Try to parse posts' date and save as UNIX timestamps.
    In combination with `-a' and `-c' you may change TimeZone to UTC and
    set date format in profile settings to:

      Y-m-d H:i:s T

  -h, --help
    This message
```

Scrape forum #13 completely and separate topics #32-40 and #70 using 100 workers:
```
$ phpbb-scraper.py -w 100 -f 13 -f 32-40,70 https://my-best-php-bb-forum.net
```
