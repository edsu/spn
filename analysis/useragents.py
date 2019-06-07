#!/usr/bin/env python
# coding: utf-8

# # User Agents in SavePageNow
# 
# This notebook examines the User Agents that were used when asking for content to be saved at Internet Archive's SavePageNow.
# 
# The User-Agent string has been used since the early days of the web as a way of identifying the software that is being used to access a web page. It is currently defined by [RFC 7231](https://tools.ietf.org/html/rfc7231#section-5.5.3) as a set of one ore more *product identifiers* which identify the software being used to perform the HTTP request.
# 
#     User-Agent      = product *( RWS ( product / comment ) )
#     product         = token ["/" product-version]
#     product-version = token
#     
# So for example the browser I am using now has this User-Agent:
# 
#     Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0
#     
# The product identifiers here are typically listed in increasing levels of specificity. 
# 
# * Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0)
# * Gecko/20100101
# * Firefox/66.0
# 
# But they are freeform enough that the [ua-parser](https://www.uaparser.org/) project has built up a voluminous set of regular expressions to parse and make sense of them.
# 
# The question is, what can the User-Agent's in the SavePageNow data tell us about the people who are participating in creation of the Internet Archive.
# 
# ## Analyze
# 
# To find the User-Agents that were used to archive the web content we need to excavate the SavePageNow WARC data. WARC files store verious [types of records](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#warc-type-mandatory). In this case we need to find the *Request* records that represent the HTTP request that was used by SavePageNow to archive the content from the Web. Using Spark with warcio is one way of doing this analysis:

# In[1]:


import sys
sys.path.append('../utils')

from warc_spark import init, extractor
sc, sqlc = init()


# An extractor function to get the User-Agent from the *WARC Request* objects:

# In[3]:


@extractor
def ua(rec):
    if rec.rec_type == 'request':
        yield rec.http_headers.get('user-agent', '')


# Now let's create a function that will run a Spark job to get the User-Agent counts for a year as a dictionary.

# In[4]:


from glob import glob

def get_year(year):
    warc_files = glob('warcs/liveweb-{}*/*.warc.gz'.format(year))
    warcs = sc.parallelize(warc_files)
    output = warcs.mapPartitions(ua)
    return output.countByValue()


# Now we can use `get_year` for each year of data we have for SPN and create a pandas DataFrame of the results.

# In[ ]:


import pandas

ua_data = {}
for year in range(2013, 2019):
    print(year)
    ua_data[str(year)] = get_year(year)

df = pandas.DataFrame(ua_data)
df.index.name = 'ua'


# We can quickly examine it to make sure it looks like it worrked:

# In[ ]:


df.head()


# Let's definitely save it for later, since it took about 6 hours to compute on XSEDE!

# In[ ]:


df.to_csv('results/ua.csv')


# ## Top 10 User Agents
# 
# Let's see if we can find any rhyme or reason in this data. If the CSV has already been calculated we can load it here instead of waiting 6 hours for the analysis to run.

# In[1]:


import pandas

df = pandas.read_csv('results/ua.csv', index_col='ua')


# What are the top 10 per year? We can iterate through the years and generate a table from our DataFrame for each year:

# In[2]:


from IPython.display import display, Markdown

# create chunks of Markdown tables to display

for year in range(2013, 2019):
    y = str(year)
    md_lines = [
    "### {}".format(y),
    "| user-agent | count |",
    "| ---------- | ----- |"
]
    dfy = df.sort_values(by=y, ascending=False)
    for ua, count in dfy[y].head(10).items():
        if count > 0:
            md_lines.append("| {} | {} |".format(ua, count))
    display(Markdown("\n".join(md_lines)))


# ## Shifting Infrastructure
# 
# A few interesting things pop out from just looking at the top 10 User-Agents by year. The first is that SavePageNow itself as a piece of software infrastructure has changed over the years. In 2013 it archived content from the web as
# 
#     Mozilla/5.0 (compatible; archive.org_bot; Wayback Machine Live Record;
#     
# So as SavePageNow received requests from users to archive web content it simply went and requested those items from the web using a fixed User-Agent string. There is only one type of User-Agent that appears in all the HTTP requests in the WARC data for 2013 (or at least the day we sampled for that year).
# 
# But in 2014 the User-Agent strings are much more varied. For example the top User-Agent was:
# 
#     Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36 (via Wayback Save Page)`.
# 
# This User-Agent string identifies a particular vesion of Chrome running on Windows. But also notice that appended on the end of this and other User-Agents in the list is the string `(via Wayback Save Page)`. It looks like in 2014 SavePageNow started using the User-Agent of the browser that original requestor, added an identifier for SavePageNow to the end, and then used that when requesting the web resource to archive.
# 
# So in theory, web servers that log User-Agents would see User-Agents of that form in their server logs when SavePageNow was archiving content on their site.
# 
# But then in 2016 things shift again and the top User-Agent changes to:
# 
#     Mozilla/5.0 (compatible; archive.org_bot; Wayback Machine Live Record; +http://archive.org/details/archive.org_bot)
#     
# This User-Agent uniquely identifies *Wayback Machine Live Record* as a generic web client. While this is the #1 user agent for 2016, 2017, and 2018 it is always followed by a long tail of User-Agent strings that uniquely identifies a type of web client software, but the `(via Wayback Save Page)` has been dropped from the end. For example:
# 
#     Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36 	
# 
# What might be going on here? 	
# 
# One explanation is that in 2016 the SavePageNow software was adjusted so that when it archived web content it no longer appended `(via Wayback Save Page)` to the original User-Agent that requested the archive. It simply passed that original User-Agent through to its final destination.
# 
# However the large number of `Wayback Machine Live Record` User-Agents is a bit of a mystery. When a browser requests something be archived could it be that request is passed through with the original User-Agent, but when it receives the HTML for that web page, and buids the DOM for it, resolving URLs for images, CSS, and JavaScript those subsequent requests get the `Wayback Machine Live Record` User-Agent when passing through SavePageNow? Or perhaps `Wayback Machine Live Record` is only used when no User-Agent is supplied initially? User-Agent is optional, and there appear to be no WARC records that lack it.
# 
# It's difficult to say without taking a much closer look at the WARC data. Which we will be doing in another notebook. It is interesting how in trying to use SavePageNow WARC data to understand the users of SavePageNow we've fallen down the rabbit hole of trying to understand the SavePageNow as a technical artifact.
# 

# ## The Long-ish Tail
# 
# Looking at the top 10 User-Agent was a useful start. But it also could be obscuring some patterns. So how many distinct User-Agent strings were there?
# 

# In[12]:


len(df.index)


# 93,843 is a lot of User-Agent strings! It might be interesting to examine what proportion of all the requests for a given year were from user agents in the top 10 for that year. To do that we need to massage our dataframe into a new dataframe so that we can easily visualize it with [Altair](https://altair-viz.github.io/).

# In[7]:


ratios = {'bucket': [], 'year': [], 'requests': []}

for year in range(2013, 2019):
    y = str(year)
    dfy = df.sort_values(by=y, ascending=False)
    
    ratios['year'].append(y)
    ratios['bucket'].append('top-10')
    ratios['requests'].append(dfy[y].head(10).sum())
    
    ratios['year'].append(y)
    ratios['bucket'].append('the-rest')
    ratios['requests'].append(dfy[y][10:].sum())

ratios = pandas.DataFrame(ratios)
ratios.head()


# In[18]:


import altair
altair.renderers.enable('notebook')

chart1 = altair.Chart(ratios).mark_bar(size=20).encode(
    altair.X('sum(requests)', title='Archive Requests'),
    altair.Y('year', title='Year'),    
    altair.Color('bucket', title='type', scale=altair.Scale(scheme='tableau20'))
)
chart1 = chart1.properties(
    width=600,
    height=175,
    title='User Agents Diversity per Year'
)
chart1.interactive()


# This is a helpful visualization because it shows that most of the time (except for year 1 when there is only one type of User-Agent) the top-10 User-Agents account for about 1/2 of the total requests. So there is definitely some diversity that's worth accounting for.
# 
# But 2017 is unusual in that by far the top-10 User-Agents account for the majority of the requests. Also there appears to have been almost double the amount of WARC request records that year, as compared to the year following. This is counter-intuitive because the amount of WARC data on disk does not follow that same trend. 

# In[10]:


from os.path import getsize

sizes = {'year': [], 'gb': []}
for year in range(2013, 2019):
    sizes['year'].append(str(year))
    warc_files = glob('warcs/liveweb-{}*/*.warc.gz'.format(year))
    sizes['gb'].append(sum([getsize(path) for path in warc_files]) / (1024 ** 3))
sizes = pandas.DataFrame(sizes)
    
chart2 = altair.Chart(sizes).mark_bar(size=20).encode(
    altair.X('gb', title='Gigabytes'),
    altair.Y('year', title='Year')
)
chart2 = chart2.properties(title='SPN WARC Data by Year', width=600, height=150)
chart2


# ## Lumping
# 
# As mentioned above there is some method in the madness of these strings. Fortunately the [ua-parser](https://www.uaparser.org/) project has built a dataset uf UserAgents, and used it to create a parser toolkit for various languages. There is one for Python that we can use to try to lump some of this data together by type of browser.
# 
# For example we can give ua_parser this string, and get back a dictionary with keys for device, os, and user_agent.
# 
#     Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15
# 

# In[2]:


import json
from ua_parser.user_agent_parser import Parse as parse_ua

ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15'
print(json.dumps(parse_ua(ua), indent=2))


# Even if the UserAgent is a string from a client like curl or wget ua-parser still does something useful:

# In[3]:


ua = 'curl/7.55.1'
print(json.dumps(parse_ua(ua), indent=2))


# So it looks like we can use the *user_agent.family* to group the User-Agent strings into a single value that identifies the piece of software (e.g. Safari, Firefox, curl, etc). Let's use our existing data to create a new DataFrame where the index is the *family_name* and the columns are the request counts by year. Along the way we'll also populate `ua_fam_map` which is a dictionary lookup for User-Agents to easily get their family. It turns out ua-parser is kinda slow and it will be useful to be able to do this operation quickly.
# 
# *Note: if ua-parser is unable to assign a family we will keep the original user-agent string (lines 9-10).*

# In[4]:


from collections import defaultdict

ua_fams = defaultdict(defaultdict)
ua_fam_map = {}
for ua in df.index:
    
    if type(ua) != str:
        continue
        
    parsed_ua = parse_ua(ua)
    ua_fam = parsed_ua['user_agent']['family']
    if ua_fam == 'Other':
        ua_fam = ua
        
    ua_fam_map[ua] = ua_fam
        
    for year in range(2013, 2019):
        y = str(year)
        if df.loc[ua][y] > 0:
            if y in ua_fams[ua_fam]:
                ua_fams[ua_fam][y] += df.loc[ua][y]
            else:
                ua_fams[ua_fam][y] = df.loc[ua][y]

ua_fams = pandas.DataFrame.from_dict(ua_fams, orient='index')


# What do the top-10 User-Agent families look like?

# In[15]:


ua_fams = ua_fams.sort_values(by='2018', ascending=False)
print(ua_fams.head(10))


# How many User-Agent families are there?
# 

# In[16]:


print(len(ua_fams.index))


# So there are several orders of magnitude less User-Agents (remember the full list was 93,843. Let's run our top-10 stats again and see how it looks:

# In[19]:


ratios = {'bucket': [], 'year': [], 'requests': []}

for year in range(2013, 2019):
    y = str(year)
    dfy = ua_fams.sort_values(by=y, ascending=False)
    
    ratios['year'].append(y)
    ratios['bucket'].append('top-10')
    ratios['requests'].append(dfy[y].head(10).sum())
    
    ratios['year'].append(y)
    ratios['bucket'].append('the-rest')
    ratios['requests'].append(dfy[y][10:].sum())

ratios = pandas.DataFrame(ratios)

chart3 = altair.Chart(ratios).mark_bar(size=20).encode(
    altair.X('sum(requests)', title='Archive Requests'),
    altair.Y('year', title='Year'),    
    altair.Color('bucket', title='type', scale=altair.Scale(scheme='tableau20'))
)
chart3 = chart3.properties(
    width=600,
    height=175,
    title='User Agent Families Diversity per Year'
)
chart3


# Nice, so our long tail of User-Agents may be long, but it doesn't account for many requests relative to the total volume. So what is the full set of top-10 User-Agents?

# In[25]:


top_10_uas = set()
for year in range(2013, 2019):
    y = str(year)
    for ua in ua_fams.sort_values(by=y, ascending=False).head(10).index:
        top_10_uas.add(ua)

top_10_uas


# One thing we can do to simplify even more is group the User-Agent families at face value based on whether they appear to be a browser or they appear to be a bot:

# In[21]:


bots = [
    "OpenBSD ftp"
    "Python Requests",
    "UptimeRobot",
    "Wget",
    "archive.org_bot",
    "curl",
    "okhttp"
]

bot_ratios = {"bucket": [], "year": [], "requests": []}

for ua in top_10_uas:
    if ua in bots:
        bucket = "bot"
    else:
        bucket = "not"
    for year in range(2013, 2019):
        y = str(year)
        bot_ratios['bucket'].append(bucket)
        bot_ratios['year'].append(y)
        bot_ratios['requests'].append(ua_fams.loc[ua][y])

bot_ratios = pandas.DataFrame(bot_ratios)
bot_ratios


# In[22]:


chart4 = altair.Chart(bot_ratios).mark_bar(size=20).encode(
    altair.X('sum(requests)', title='Archive Requests'),
    altair.Y('year', title='Year'),    
    altair.Color('bucket', title='type', scale=altair.Scale(scheme='tableau20'))
)
chart4 = chart4.properties(
    width=600,
    height=175,
    title='Bot vs Human SPN Requests'
)
chart4


# So if you remember the anomalous number of requests in 2017 we can see that bots appear to have been responsible. The proportion of bot User-Agents is much greater in 2017. In fact all we have to do is look at the top User-Agent for that year, and it tells us which one is responsible:

# In[41]:


dfy = df.sort_values('2017', ascending=False)
first = dfy['2017'][0]
print(dfy.index[0], dfy['2017'][0])


# Yes. it's the mysterious `Wayback Machine Live Record`. At 6 million requests it amounts for the majority of the bots in 2017--about half of the additional traffic. But what were these bots doing? Can we infer anything about what types of material was being archived by the two types of agents? Answering these questions is a topic for another notebook. But for now let's save a mapping of User-Agents to their family so we can use it later when processing WARC data.

# In[23]:


json.dump(ua_fam_map, open('results/ua-families.json', 'w'), indent=2)


# And we might as well save our top-10 User-Agents per year, along with whether we classed it as a bot or not.

# In[2]:


top_uas = {ua: ua in bots for ua in top_10_uas}
json.dump(top_uas, open('results/top-uas.json', 'w'), indent=2)
top_uas


# In[ ]:




