# ImgSys_XtrProc

xtr_rnm_anlz_fltr_cnvrt_pkg::img_flow
src{TAR|ZIP|HF}->[bid000000++]->[webp:q90]->[zip:out]
crit{dim>=800px,qual=orig,clean=src}
cfg{||proc,>>chkpt,*db,#prog}

## _struct_
```
/
├─src/
│ ├─batch_proc.py 
│ ├─phs/{xtr,rnm,anlz,fltr,cnv,pkg}.py
│ └─utl/{img,rpt,db,hf,str,prg}.py
├─{cfg,dat,out,db,chk}/
└─main.py
```

## _comps_
```python
# DSM: *<-{lcl|hf}
DSM.p_src() -> {l|h}
DSM.h_hf() -> v+g

# PM: [ ===== ]
PM.s(p,t) -> b 
PM.u(n,**) -> p+s
PM.f() -> s

# SM: del+zip 
SM.cl(s) -> -s
SM.pk(b,d,r) -> +z

# DBM: ^all
DBM.en() -> tb++
DBM.a_b(b,p) -> +b
DBM.rg(b,i,n) -> +i
DBM.up(i,s,**) -> ~i

# CM: >>if!
CM.sv(p,d) -> +f
CM.ld() -> <-f
CM.ex() -> ?f

# PP: ||items
PP.pc(i,f,*,**) -> r[]

# BP: *all
BP.p() -> <<
BP.x() -> s>f
BP.r() -> f>bid+
BP.a() -> =s
BP.f() -> d>=800
BP.c() -> q=90
```

## _cfg_
```json
{
  "min_res": 800,
  "out_fmt": "webp",
  "qual": 90,
  "bid_pfx": "b",
  "||": {"mx_w": 8, "p": true},
  ">>": {"en": true},
  "*": {"pth": "db/i.db"},
  "@": {"cch": "d/hf"},
  "-+": {"del": true, "pkg": true},
  "#": {"prg": true, "st": true}
}
```

## _flow_
1. ?{local|hf+^+@}
2. !{bid+out+par}
3. *{xtr->rnm->anlz->fltr->cnv->pkg}
4. >>{webp+rej+csv->zip}

## _run_
`py main.py` -> !+#