def stilut2json(stilut):
    sid = 1
    tid = 1
    iid = 1
    pid = 1
    s = dict() # s[site] => sid
    t = dict()
    i = dict()
    pfx = list()
    fix = list()
    for (site,tele,inst),prefix in stilut.items():
        if site not in s:
            s[site] = sid
            sid += 1
        if tele not in t:
            t[tele] = tid
            tid += 1
        if inst not in i:
            i[inst] = iid
            iid += 1
        pfx.append(dict(model = 'tada.fileprefix',
                        pk = pid,
                        fields = dict(site = s[site],
                                      telescope = t[tele],
                                      instrument = i[inst],
                                      prefix = prefix
                        )))
        pid += 1
    for k,v in s.items():
        fix.append(dict(model = 'tada.site',
                        pk = v,
                        fields = dict(name = k)))
    for k,v in t.items():
        fix.append(dict(model = 'tada.telescope',
                        pk = v,
                        fields = dict(name = k)))
    for k,v in i.items():
        fix.append(dict(model = 'tada.instrument',
                        pk = v,
                        fields = dict(name = k)))
    fix.extend(pfx)
    return fix
