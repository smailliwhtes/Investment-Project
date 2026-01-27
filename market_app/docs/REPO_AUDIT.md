# Repository Audit

## Overview

This audit inventories every tracked file in the repository and summarizes its purpose.
Detailed per-file metadata (including key functions/classes and CLI usage) is captured in
`docs/file_manifest.json`.

## Missing vs. Blueprint

- Regime overlay + analog expansion (PR #2) is still pending for the advanced module phase.
- Optional online data providers remain gated behind explicit configuration and API keys; offline is the default.
- No dedicated `provision_data.ps1` exists yet; data provisioning relies on documented folder paths.

## Per-File Inventory

| Path | Purpose | CLI Usage |
| --- | --- | --- |
| .env | Repository metadata or auxiliary file. | none |
| .env.example | Repository metadata or auxiliary file. | none |
| .gitattributes | Repository metadata or auxiliary file. | none |
| .gitignore | Repository metadata or auxiliary file. | none |
| .pre-commit-config.yaml | Repository metadata or auxiliary file. | none |
| AUDIT.md | Repository metadata or auxiliary file. | none |
| FIX_PLAN.md | Repository metadata or auxiliary file. | none |
| Finnhub_scripts.txt | Repository metadata or auxiliary file. | none |
| README.md | Repository metadata or auxiliary file. | none |
| _backup/20260111_205832/config.json | Historical backup artifact. | none |
| _backup/20260111_205832/doctor.ps1 | PowerShell automation script. | none |
| _backup/20260111_205832/requirements.txt | Historical backup artifact. | none |
| _backup_doctor_preflight/doctor_20260111_223421.ps1 | PowerShell automation script. | none |
| _backup_domain_swap/runner_20260111_222903.py | Historical backup artifact. | none |
| _backup_fix/20260111_210644/config.json | Historical backup artifact. | none |
| _backup_fix/20260111_210644/doctor.ps1 | PowerShell automation script. | none |
| _backup_fix/20260111_210644/scripts/runner.py | Historical backup artifact. | none |
| _backup_fix2/20260111_211117/doctor.ps1 | PowerShell automation script. | none |
| _backup_limittexts/runner_20260111_223357.py | Historical backup artifact. | none |
| _backup_limittexts/runner_20260111_223816.py | Historical backup artifact. | none |
| _backup_progress/20260111_211834/doctor.ps1 | PowerShell automation script. | none |
| _backup_progress/20260111_211834/scripts/runner.py | Historical backup artifact. | none |
| _backup_stooq_limitfix/20260111_215719/config.json | Historical backup artifact. | none |
| _backup_stooq_limitfix/20260111_215719/runner.py | Historical backup artifact. | none |
| _backup_stooqlog/20260111_214613/runner.py | Historical backup artifact. | none |
| acceptance_test.ps1 | PowerShell automation script. | none |
| config.example.yaml | Configuration file for pipeline or tooling. | none |
| config.json | Configuration file for pipeline or tooling. | none |
| config.json.bak | Repository metadata or auxiliary file. | none |
| config.json.bak_20260117_222949 | Repository metadata or auxiliary file. | none |
| config.json.bak_20260117_224501 | Repository metadata or auxiliary file. | none |
| config.json.bak_20260117_225034 | Repository metadata or auxiliary file. | none |
| config.json.bak_20260117_230234 | Repository metadata or auxiliary file. | none |
| config.json.bak_20260117_231512 | Repository metadata or auxiliary file. | none |
| config.yaml | Configuration file for pipeline or tooling. | none |
| config/config.yaml | Configuration file for pipeline or tooling. | none |
| config/logging.yaml | Configuration file for pipeline or tooling. | none |
| config/sources.yaml | Configuration file for pipeline or tooling. | none |
| config/watchlists.yaml | Configuration file for pipeline or tooling. | none |
| data/state/batch_state.json | Data asset or state snapshot. | none |
| data/universe/universe.csv | Data asset or state snapshot. | none |
| data/watchlist.txt | Data asset or state snapshot. | none |
| data_cache/stooq/AAL.csv | Cached data sample (offline). | none |
| data_cache/stooq/AALG.csv | Cached data sample (offline). | none |
| data_cache/stooq/AAPB.csv | Cached data sample (offline). | none |
| data_cache/stooq/AAPD.csv | Cached data sample (offline). | none |
| data_cache/stooq/AAPU.csv | Cached data sample (offline). | none |
| data_cache/stooq/ABNB.csv | Cached data sample (offline). | none |
| data_cache/stooq/ABNG.csv | Cached data sample (offline). | none |
| data_cache/stooq/ADBG.csv | Cached data sample (offline). | none |
| data_cache/stooq/AENT.csv | Cached data sample (offline). | none |
| data_cache/stooq/AFJK.csv | Cached data sample (offline). | none |
| data_cache/stooq/AGAE.csv | Cached data sample (offline). | none |
| data_cache/stooq/AGMI.csv | Cached data sample (offline). | none |
| data_cache/stooq/AGPU.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIFU.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIIO.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIMD.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIOT.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIPI.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIPO.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIRG.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIRJ.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIRO.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIRR.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIRS.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIRT.csv | Cached data sample (offline). | none |
| data_cache/stooq/AISP.csv | Cached data sample (offline). | none |
| data_cache/stooq/AIXC.csv | Cached data sample (offline). | none |
| data_cache/stooq/AKAM.csv | Cached data sample (offline). | none |
| data_cache/stooq/AMCI.csv | Cached data sample (offline). | none |
| data_cache/stooq/AMDD.csv | Cached data sample (offline). | none |
| data_cache/stooq/AMDG.csv | Cached data sample (offline). | none |
| data_cache/stooq/AMDL.csv | Cached data sample (offline). | none |
| data_cache/stooq/AMUU.csv | Cached data sample (offline). | none |
| data_cache/stooq/AMZD.csv | Cached data sample (offline). | none |
| data_cache/stooq/AMZU.csv | Cached data sample (offline). | none |
| data_cache/stooq/AMZZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/ANEL.csv | Cached data sample (offline). | none |
| data_cache/stooq/AOSL.csv | Cached data sample (offline). | none |
| data_cache/stooq/APPX.csv | Cached data sample (offline). | none |
| data_cache/stooq/ARAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/ARBE.csv | Cached data sample (offline). | none |
| data_cache/stooq/ARMG.csv | Cached data sample (offline). | none |
| data_cache/stooq/ASMG.csv | Cached data sample (offline). | none |
| data_cache/stooq/ASTL.csv | Cached data sample (offline). | none |
| data_cache/stooq/ASTS.csv | Cached data sample (offline). | none |
| data_cache/stooq/ATAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/ATLX.csv | Cached data sample (offline). | none |
| data_cache/stooq/AUMI.csv | Cached data sample (offline). | none |
| data_cache/stooq/AVGG.csv | Cached data sample (offline). | none |
| data_cache/stooq/AVGU.csv | Cached data sample (offline). | none |
| data_cache/stooq/AVGX.csv | Cached data sample (offline). | none |
| data_cache/stooq/AVL.csv | Cached data sample (offline). | none |
| data_cache/stooq/AVS.csv | Cached data sample (offline). | none |
| data_cache/stooq/AVXX.csv | Cached data sample (offline). | none |
| data_cache/stooq/BABX.csv | Cached data sample (offline). | none |
| data_cache/stooq/BAER.csv | Cached data sample (offline). | none |
| data_cache/stooq/BAIG.csv | Cached data sample (offline). | none |
| data_cache/stooq/BASG.csv | Cached data sample (offline). | none |
| data_cache/stooq/BASV.csv | Cached data sample (offline). | none |
| data_cache/stooq/BBB.csv | Cached data sample (offline). | none |
| data_cache/stooq/BDGS.csv | Cached data sample (offline). | none |
| data_cache/stooq/BDMD.csv | Cached data sample (offline). | none |
| data_cache/stooq/BEG.csv | Cached data sample (offline). | none |
| data_cache/stooq/BFRG.csv | Cached data sample (offline). | none |
| data_cache/stooq/BGIN.csv | Cached data sample (offline). | none |
| data_cache/stooq/BGL.csv | Cached data sample (offline). | none |
| data_cache/stooq/BHAT.csv | Cached data sample (offline). | none |
| data_cache/stooq/BIDG.csv | Cached data sample (offline). | none |
| data_cache/stooq/BITS.csv | Cached data sample (offline). | none |
| data_cache/stooq/BIYA.csv | Cached data sample (offline). | none |
| data_cache/stooq/BKCH.csv | Cached data sample (offline). | none |
| data_cache/stooq/BLDP.csv | Cached data sample (offline). | none |
| data_cache/stooq/BLFY.csv | Cached data sample (offline). | none |
| data_cache/stooq/BLSG.csv | Cached data sample (offline). | none |
| data_cache/stooq/BLZR.csv | Cached data sample (offline). | none |
| data_cache/stooq/BMNG.csv | Cached data sample (offline). | none |
| data_cache/stooq/BNZI.csv | Cached data sample (offline). | none |
| data_cache/stooq/BOED.csv | Cached data sample (offline). | none |
| data_cache/stooq/BOEG.csv | Cached data sample (offline). | none |
| data_cache/stooq/BOEU.csv | Cached data sample (offline). | none |
| data_cache/stooq/BOTT.csv | Cached data sample (offline). | none |
| data_cache/stooq/BOTZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/BRFH.csv | Cached data sample (offline). | none |
| data_cache/stooq/BRKD.csv | Cached data sample (offline). | none |
| data_cache/stooq/BRKU.csv | Cached data sample (offline). | none |
| data_cache/stooq/BTGD.csv | Cached data sample (offline). | none |
| data_cache/stooq/BU.csv | Cached data sample (offline). | none |
| data_cache/stooq/BUG.csv | Cached data sample (offline). | none |
| data_cache/stooq/BULG.csv | Cached data sample (offline). | none |
| data_cache/stooq/BULX.csv | Cached data sample (offline). | none |
| data_cache/stooq/BZAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/CAKE.csv | Cached data sample (offline). | none |
| data_cache/stooq/CALM.csv | Cached data sample (offline). | none |
| data_cache/stooq/CCLD.csv | Cached data sample (offline). | none |
| data_cache/stooq/CCSI.csv | Cached data sample (offline). | none |
| data_cache/stooq/CD.csv | Cached data sample (offline). | none |
| data_cache/stooq/CENX.csv | Cached data sample (offline). | none |
| data_cache/stooq/CHAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/CHGX.csv | Cached data sample (offline). | none |
| data_cache/stooq/CHPS.csv | Cached data sample (offline). | none |
| data_cache/stooq/CHPX.csv | Cached data sample (offline). | none |
| data_cache/stooq/CIBR.csv | Cached data sample (offline). | none |
| data_cache/stooq/CIFG.csv | Cached data sample (offline). | none |
| data_cache/stooq/CLFD.csv | Cached data sample (offline). | none |
| data_cache/stooq/CLOD.csv | Cached data sample (offline). | none |
| data_cache/stooq/CLOU.csv | Cached data sample (offline). | none |
| data_cache/stooq/CMGG.csv | Cached data sample (offline). | none |
| data_cache/stooq/CNCG.csv | Cached data sample (offline). | none |
| data_cache/stooq/COIG.csv | Cached data sample (offline). | none |
| data_cache/stooq/CONI.csv | Cached data sample (offline). | none |
| data_cache/stooq/CONL.csv | Cached data sample (offline). | none |
| data_cache/stooq/CONX.csv | Cached data sample (offline). | none |
| data_cache/stooq/COPJ.csv | Cached data sample (offline). | none |
| data_cache/stooq/COPP.csv | Cached data sample (offline). | none |
| data_cache/stooq/COTG.csv | Cached data sample (offline). | none |
| data_cache/stooq/CPBI.csv | Cached data sample (offline). | none |
| data_cache/stooq/CRCG.csv | Cached data sample (offline). | none |
| data_cache/stooq/CRMG.csv | Cached data sample (offline). | none |
| data_cache/stooq/CRSR.csv | Cached data sample (offline). | none |
| data_cache/stooq/CRWG.csv | Cached data sample (offline). | none |
| data_cache/stooq/CRWL.csv | Cached data sample (offline). | none |
| data_cache/stooq/CSAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/CSCL.csv | Cached data sample (offline). | none |
| data_cache/stooq/CSCS.csv | Cached data sample (offline). | none |
| data_cache/stooq/CTNT.csv | Cached data sample (offline). | none |
| data_cache/stooq/CVNX.csv | Cached data sample (offline). | none |
| data_cache/stooq/CYBR.csv | Cached data sample (offline). | none |
| data_cache/stooq/CZR.csv | Cached data sample (offline). | none |
| data_cache/stooq/DJCO.csv | Cached data sample (offline). | none |
| data_cache/stooq/DKNX.csv | Cached data sample (offline). | none |
| data_cache/stooq/DLLL.csv | Cached data sample (offline). | none |
| data_cache/stooq/DLPN.csv | Cached data sample (offline). | none |
| data_cache/stooq/DSY.csv | Cached data sample (offline). | none |
| data_cache/stooq/DTCR.csv | Cached data sample (offline). | none |
| data_cache/stooq/DTSQ.csv | Cached data sample (offline). | none |
| data_cache/stooq/DUOG.csv | Cached data sample (offline). | none |
| data_cache/stooq/DVLT.csv | Cached data sample (offline). | none |
| data_cache/stooq/DYTA.csv | Cached data sample (offline). | none |
| data_cache/stooq/EGAN.csv | Cached data sample (offline). | none |
| data_cache/stooq/ELIL.csv | Cached data sample (offline). | none |
| data_cache/stooq/ELIS.csv | Cached data sample (offline). | none |
| data_cache/stooq/ELSE.csv | Cached data sample (offline). | none |
| data_cache/stooq/ELVR.csv | Cached data sample (offline). | none |
| data_cache/stooq/ETRL.csv | Cached data sample (offline). | none |
| data_cache/stooq/FBL.csv | Cached data sample (offline). | none |
| data_cache/stooq/FBOT.csv | Cached data sample (offline). | none |
| data_cache/stooq/FHB.csv | Cached data sample (offline). | none |
| data_cache/stooq/FIGG.csv | Cached data sample (offline). | none |
| data_cache/stooq/FIP.csv | Cached data sample (offline). | none |
| data_cache/stooq/FLXS.csv | Cached data sample (offline). | none |
| data_cache/stooq/FLY.csv | Cached data sample (offline). | none |
| data_cache/stooq/FOXF.csv | Cached data sample (offline). | none |
| data_cache/stooq/FRDD.csv | Cached data sample (offline). | none |
| data_cache/stooq/FRDU.csv | Cached data sample (offline). | none |
| data_cache/stooq/FTAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/FTGC.csv | Cached data sample (offline). | none |
| data_cache/stooq/FTXL.csv | Cached data sample (offline). | none |
| data_cache/stooq/FUTG.csv | Cached data sample (offline). | none |
| data_cache/stooq/FWRD.csv | Cached data sample (offline). | none |
| data_cache/stooq/GAIA.csv | Cached data sample (offline). | none |
| data_cache/stooq/GANX.csv | Cached data sample (offline). | none |
| data_cache/stooq/GBUG.csv | Cached data sample (offline). | none |
| data_cache/stooq/GCT.csv | Cached data sample (offline). | none |
| data_cache/stooq/GDEN.csv | Cached data sample (offline). | none |
| data_cache/stooq/GDFN.csv | Cached data sample (offline). | none |
| data_cache/stooq/GDHG.csv | Cached data sample (offline). | none |
| data_cache/stooq/GDYN.csv | Cached data sample (offline). | none |
| data_cache/stooq/GEMG.csv | Cached data sample (offline). | none |
| data_cache/stooq/GEMI.csv | Cached data sample (offline). | none |
| data_cache/stooq/GEOS.csv | Cached data sample (offline). | none |
| data_cache/stooq/GEVG.csv | Cached data sample (offline). | none |
| data_cache/stooq/GFAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/GGLL.csv | Cached data sample (offline). | none |
| data_cache/stooq/GGLS.csv | Cached data sample (offline). | none |
| data_cache/stooq/GIFI.csv | Cached data sample (offline). | none |
| data_cache/stooq/GILT.csv | Cached data sample (offline). | none |
| data_cache/stooq/GIND.csv | Cached data sample (offline). | none |
| data_cache/stooq/GKAT.csv | Cached data sample (offline). | none |
| data_cache/stooq/GLDI.csv | Cached data sample (offline). | none |
| data_cache/stooq/GLDY.csv | Cached data sample (offline). | none |
| data_cache/stooq/GLGG.csv | Cached data sample (offline). | none |
| data_cache/stooq/GMGI.csv | Cached data sample (offline). | none |
| data_cache/stooq/GMM.csv | Cached data sample (offline). | none |
| data_cache/stooq/GOU.csv | Cached data sample (offline). | none |
| data_cache/stooq/GPIQ.csv | Cached data sample (offline). | none |
| data_cache/stooq/GPIX.csv | Cached data sample (offline). | none |
| data_cache/stooq/GPRE.csv | Cached data sample (offline). | none |
| data_cache/stooq/GRAG.csv | Cached data sample (offline). | none |
| data_cache/stooq/GRAL.csv | Cached data sample (offline). | none |
| data_cache/stooq/GRDX.csv | Cached data sample (offline). | none |
| data_cache/stooq/GRID.csv | Cached data sample (offline). | none |
| data_cache/stooq/GSGO.csv | Cached data sample (offline). | none |
| data_cache/stooq/GSUN.csv | Cached data sample (offline). | none |
| data_cache/stooq/GTOP.csv | Cached data sample (offline). | none |
| data_cache/stooq/GTPE.csv | Cached data sample (offline). | none |
| data_cache/stooq/GUSE.csv | Cached data sample (offline). | none |
| data_cache/stooq/GVLE.csv | Cached data sample (offline). | none |
| data_cache/stooq/GXAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/HAIN.csv | Cached data sample (offline). | none |
| data_cache/stooq/HCAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/HFSP.csv | Cached data sample (offline). | none |
| data_cache/stooq/HIMZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/HODU.csv | Cached data sample (offline). | none |
| data_cache/stooq/HOLO.csv | Cached data sample (offline). | none |
| data_cache/stooq/HOOG.csv | Cached data sample (offline). | none |
| data_cache/stooq/HOOX.csv | Cached data sample (offline). | none |
| data_cache/stooq/HOVR.csv | Cached data sample (offline). | none |
| data_cache/stooq/HPAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/HSPT.csv | Cached data sample (offline). | none |
| data_cache/stooq/HUBC.csv | Cached data sample (offline). | none |
| data_cache/stooq/HYLS.csv | Cached data sample (offline). | none |
| data_cache/stooq/HYP.csv | Cached data sample (offline). | none |
| data_cache/stooq/HYPR.csv | Cached data sample (offline). | none |
| data_cache/stooq/IBOT.csv | Cached data sample (offline). | none |
| data_cache/stooq/ICOP.csv | Cached data sample (offline). | none |
| data_cache/stooq/IDEF.csv | Cached data sample (offline). | none |
| data_cache/stooq/ILIT.csv | Cached data sample (offline). | none |
| data_cache/stooq/ILPT.csv | Cached data sample (offline). | none |
| data_cache/stooq/INDI.csv | Cached data sample (offline). | none |
| data_cache/stooq/INFR.csv | Cached data sample (offline). | none |
| data_cache/stooq/INSE.csv | Cached data sample (offline). | none |
| data_cache/stooq/INTW.csv | Cached data sample (offline). | none |
| data_cache/stooq/IONL.csv | Cached data sample (offline). | none |
| data_cache/stooq/IONX.csv | Cached data sample (offline). | none |
| data_cache/stooq/IONZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/IPAR.csv | Cached data sample (offline). | none |
| data_cache/stooq/IPGP.csv | Cached data sample (offline). | none |
| data_cache/stooq/IREG.csv | Cached data sample (offline). | none |
| data_cache/stooq/ISUL.csv | Cached data sample (offline). | none |
| data_cache/stooq/JBLU.csv | Cached data sample (offline). | none |
| data_cache/stooq/JTAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/JYD.csv | Cached data sample (offline). | none |
| data_cache/stooq/KALU.csv | Cached data sample (offline). | none |
| data_cache/stooq/KAT.csv | Cached data sample (offline). | none |
| data_cache/stooq/KBAB.csv | Cached data sample (offline). | none |
| data_cache/stooq/KBDU.csv | Cached data sample (offline). | none |
| data_cache/stooq/KCHV.csv | Cached data sample (offline). | none |
| data_cache/stooq/KDK.csv | Cached data sample (offline). | none |
| data_cache/stooq/KITT.csv | Cached data sample (offline). | none |
| data_cache/stooq/KJD.csv | Cached data sample (offline). | none |
| data_cache/stooq/KLAG.csv | Cached data sample (offline). | none |
| data_cache/stooq/KMLI.csv | Cached data sample (offline). | none |
| data_cache/stooq/KPDD.csv | Cached data sample (offline). | none |
| data_cache/stooq/KTOS.csv | Cached data sample (offline). | none |
| data_cache/stooq/KUST.csv | Cached data sample (offline). | none |
| data_cache/stooq/KXIN.csv | Cached data sample (offline). | none |
| data_cache/stooq/LACG.csv | Cached data sample (offline). | none |
| data_cache/stooq/LASE.csv | Cached data sample (offline). | none |
| data_cache/stooq/LAWR.csv | Cached data sample (offline). | none |
| data_cache/stooq/LCDL.csv | Cached data sample (offline). | none |
| data_cache/stooq/LEXI.csv | Cached data sample (offline). | none |
| data_cache/stooq/LINT.csv | Cached data sample (offline). | none |
| data_cache/stooq/LITP.csv | Cached data sample (offline). | none |
| data_cache/stooq/LLYZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/LMAT.csv | Cached data sample (offline). | none |
| data_cache/stooq/LMNX.csv | Cached data sample (offline). | none |
| data_cache/stooq/LMTL.csv | Cached data sample (offline). | none |
| data_cache/stooq/LMTS.csv | Cached data sample (offline). | none |
| data_cache/stooq/LNAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/LOTI.csv | Cached data sample (offline). | none |
| data_cache/stooq/LPAA.csv | Cached data sample (offline). | none |
| data_cache/stooq/LPBB.csv | Cached data sample (offline). | none |
| data_cache/stooq/LSCC.csv | Cached data sample (offline). | none |
| data_cache/stooq/LULG.csv | Cached data sample (offline). | none |
| data_cache/stooq/MBOT.csv | Cached data sample (offline). | none |
| data_cache/stooq/MCHP.csv | Cached data sample (offline). | none |
| data_cache/stooq/MDAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/METD.csv | Cached data sample (offline). | none |
| data_cache/stooq/METU.csv | Cached data sample (offline). | none |
| data_cache/stooq/MLAC.csv | Cached data sample (offline). | none |
| data_cache/stooq/MNSB.csv | Cached data sample (offline). | none |
| data_cache/stooq/MOOD.csv | Cached data sample (offline). | none |
| data_cache/stooq/MPG.csv | Cached data sample (offline). | none |
| data_cache/stooq/MPWR.csv | Cached data sample (offline). | none |
| data_cache/stooq/MRAL.csv | Cached data sample (offline). | none |
| data_cache/stooq/MRVI.csv | Cached data sample (offline). | none |
| data_cache/stooq/MSAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/MSDD.csv | Cached data sample (offline). | none |
| data_cache/stooq/MSFD.csv | Cached data sample (offline). | none |
| data_cache/stooq/MSFL.csv | Cached data sample (offline). | none |
| data_cache/stooq/MSFU.csv | Cached data sample (offline). | none |
| data_cache/stooq/MSS.csv | Cached data sample (offline). | none |
| data_cache/stooq/MSTP.csv | Cached data sample (offline). | none |
| data_cache/stooq/MSTX.csv | Cached data sample (offline). | none |
| data_cache/stooq/MUD.csv | Cached data sample (offline). | none |
| data_cache/stooq/MULL.csv | Cached data sample (offline). | none |
| data_cache/stooq/MUU.csv | Cached data sample (offline). | none |
| data_cache/stooq/MVLL.csv | Cached data sample (offline). | none |
| data_cache/stooq/MYNZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/NATO.csv | Cached data sample (offline). | none |
| data_cache/stooq/NBIG.csv | Cached data sample (offline). | none |
| data_cache/stooq/NBIL.csv | Cached data sample (offline). | none |
| data_cache/stooq/NCEW.csv | Cached data sample (offline). | none |
| data_cache/stooq/NEMG.csv | Cached data sample (offline). | none |
| data_cache/stooq/NETG.csv | Cached data sample (offline). | none |
| data_cache/stooq/NEWZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/NFXL.csv | Cached data sample (offline). | none |
| data_cache/stooq/NFXS.csv | Cached data sample (offline). | none |
| data_cache/stooq/NIKL.csv | Cached data sample (offline). | none |
| data_cache/stooq/NIOG.csv | Cached data sample (offline). | none |
| data_cache/stooq/NMFC.csv | Cached data sample (offline). | none |
| data_cache/stooq/NNE.csv | Cached data sample (offline). | none |
| data_cache/stooq/NOWL.csv | Cached data sample (offline). | none |
| data_cache/stooq/NSCR.csv | Cached data sample (offline). | none |
| data_cache/stooq/NSI.csv | Cached data sample (offline). | none |
| data_cache/stooq/NSSC.csv | Cached data sample (offline). | none |
| data_cache/stooq/NUG.csv | Cached data sample (offline). | none |
| data_cache/stooq/NUGY.csv | Cached data sample (offline). | none |
| data_cache/stooq/NVD.csv | Cached data sample (offline). | none |
| data_cache/stooq/NVDD.csv | Cached data sample (offline). | none |
| data_cache/stooq/NVDG.csv | Cached data sample (offline). | none |
| data_cache/stooq/NVDL.csv | Cached data sample (offline). | none |
| data_cache/stooq/NVDS.csv | Cached data sample (offline). | none |
| data_cache/stooq/NVDU.csv | Cached data sample (offline). | none |
| data_cache/stooq/NVTS.csv | Cached data sample (offline). | none |
| data_cache/stooq/NXPI.csv | Cached data sample (offline). | none |
| data_cache/stooq/OCC.csv | Cached data sample (offline). | none |
| data_cache/stooq/ODDS.csv | Cached data sample (offline). | none |
| data_cache/stooq/ODYS.csv | Cached data sample (offline). | none |
| data_cache/stooq/OKLL.csv | Cached data sample (offline). | none |
| data_cache/stooq/OKTG.csv | Cached data sample (offline). | none |
| data_cache/stooq/OLLI.csv | Cached data sample (offline). | none |
| data_cache/stooq/ON.csv | Cached data sample (offline). | none |
| data_cache/stooq/OPEG.csv | Cached data sample (offline). | none |
| data_cache/stooq/ORCS.csv | Cached data sample (offline). | none |
| data_cache/stooq/ORCU.csv | Cached data sample (offline). | none |
| data_cache/stooq/ORCX.csv | Cached data sample (offline). | none |
| data_cache/stooq/OSCG.csv | Cached data sample (offline). | none |
| data_cache/stooq/OSCX.csv | Cached data sample (offline). | none |
| data_cache/stooq/OTTR.csv | Cached data sample (offline). | none |
| data_cache/stooq/PAGP.csv | Cached data sample (offline). | none |
| data_cache/stooq/PAL.csv | Cached data sample (offline). | none |
| data_cache/stooq/PALD.csv | Cached data sample (offline). | none |
| data_cache/stooq/PALU.csv | Cached data sample (offline). | none |
| data_cache/stooq/PANG.csv | Cached data sample (offline). | none |
| data_cache/stooq/PANL.csv | Cached data sample (offline). | none |
| data_cache/stooq/PAVS.csv | Cached data sample (offline). | none |
| data_cache/stooq/PBRG.csv | Cached data sample (offline). | none |
| data_cache/stooq/PDDL.csv | Cached data sample (offline). | none |
| data_cache/stooq/PDYN.csv | Cached data sample (offline). | none |
| data_cache/stooq/PENN.csv | Cached data sample (offline). | none |
| data_cache/stooq/PGJ.csv | Cached data sample (offline). | none |
| data_cache/stooq/PLAY.csv | Cached data sample (offline). | none |
| data_cache/stooq/PLTD.csv | Cached data sample (offline). | none |
| data_cache/stooq/PLTG.csv | Cached data sample (offline). | none |
| data_cache/stooq/PLTU.csv | Cached data sample (offline). | none |
| data_cache/stooq/PLTZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/PRCT.csv | Cached data sample (offline). | none |
| data_cache/stooq/PRFX.csv | Cached data sample (offline). | none |
| data_cache/stooq/PROP.csv | Cached data sample (offline). | none |
| data_cache/stooq/PSHG.csv | Cached data sample (offline). | none |
| data_cache/stooq/PSWD.csv | Cached data sample (offline). | none |
| data_cache/stooq/PTIR.csv | Cached data sample (offline). | none |
| data_cache/stooq/PYPG.csv | Cached data sample (offline). | none |
| data_cache/stooq/QBTZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/QCMD.csv | Cached data sample (offline). | none |
| data_cache/stooq/QCML.csv | Cached data sample (offline). | none |
| data_cache/stooq/QCMU.csv | Cached data sample (offline). | none |
| data_cache/stooq/QPUX.csv | Cached data sample (offline). | none |
| data_cache/stooq/QTR.csv | Cached data sample (offline). | none |
| data_cache/stooq/RAIN.csv | Cached data sample (offline). | none |
| data_cache/stooq/RBIL.csv | Cached data sample (offline). | none |
| data_cache/stooq/RCKT.csv | Cached data sample (offline). | none |
| data_cache/stooq/RCT.csv | Cached data sample (offline). | none |
| data_cache/stooq/RDTL.csv | Cached data sample (offline). | none |
| data_cache/stooq/RFAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/RFDI.csv | Cached data sample (offline). | none |
| data_cache/stooq/RFEM.csv | Cached data sample (offline). | none |
| data_cache/stooq/RFEU.csv | Cached data sample (offline). | none |
| data_cache/stooq/RFIL.csv | Cached data sample (offline). | none |
| data_cache/stooq/RGLD.csv | Cached data sample (offline). | none |
| data_cache/stooq/RGTX.csv | Cached data sample (offline). | none |
| data_cache/stooq/RGTZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/RING.csv | Cached data sample (offline). | none |
| data_cache/stooq/RJET.csv | Cached data sample (offline). | none |
| data_cache/stooq/RKLB.csv | Cached data sample (offline). | none |
| data_cache/stooq/RKLX.csv | Cached data sample (offline). | none |
| data_cache/stooq/RKLZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/RMCF.csv | Cached data sample (offline). | none |
| data_cache/stooq/ROBT.csv | Cached data sample (offline). | none |
| data_cache/stooq/RR.csv | Cached data sample (offline). | none |
| data_cache/stooq/RTAC.csv | Cached data sample (offline). | none |
| data_cache/stooq/RTH.csv | Cached data sample (offline). | none |
| data_cache/stooq/RTXG.csv | Cached data sample (offline). | none |
| data_cache/stooq/RVNL.csv | Cached data sample (offline). | none |
| data_cache/stooq/RVSN.csv | Cached data sample (offline). | none |
| data_cache/stooq/RXT.csv | Cached data sample (offline). | none |
| data_cache/stooq/RYET.csv | Cached data sample (offline). | none |
| data_cache/stooq/RZLV.csv | Cached data sample (offline). | none |
| data_cache/stooq/SAIA.csv | Cached data sample (offline). | none |
| data_cache/stooq/SAIH.csv | Cached data sample (offline). | none |
| data_cache/stooq/SAIL.csv | Cached data sample (offline). | none |
| data_cache/stooq/SAMG.csv | Cached data sample (offline). | none |
| data_cache/stooq/SARK.csv | Cached data sample (offline). | none |
| data_cache/stooq/SATG.csv | Cached data sample (offline). | none |
| data_cache/stooq/SBGI.csv | Cached data sample (offline). | none |
| data_cache/stooq/SBU.csv | Cached data sample (offline). | none |
| data_cache/stooq/SDG.csv | Cached data sample (offline). | none |
| data_cache/stooq/SEMY.csv | Cached data sample (offline). | none |
| data_cache/stooq/SERV.csv | Cached data sample (offline). | none |
| data_cache/stooq/SGML.csv | Cached data sample (offline). | none |
| data_cache/stooq/SHPD.csv | Cached data sample (offline). | none |
| data_cache/stooq/SHPU.csv | Cached data sample (offline). | none |
| data_cache/stooq/SIDU.csv | Cached data sample (offline). | none |
| data_cache/stooq/SKRE.csv | Cached data sample (offline). | none |
| data_cache/stooq/SKYU.csv | Cached data sample (offline). | none |
| data_cache/stooq/SKYY.csv | Cached data sample (offline). | none |
| data_cache/stooq/SLGB.csv | Cached data sample (offline). | none |
| data_cache/stooq/SLVO.csv | Cached data sample (offline). | none |
| data_cache/stooq/SLVR.csv | Cached data sample (offline). | none |
| data_cache/stooq/SMCI.csv | Cached data sample (offline). | none |
| data_cache/stooq/SMCL.csv | Cached data sample (offline). | none |
| data_cache/stooq/SMCX.csv | Cached data sample (offline). | none |
| data_cache/stooq/SMCZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/SMH.csv | Cached data sample (offline). | none |
| data_cache/stooq/SMHX.csv | Cached data sample (offline). | none |
| data_cache/stooq/SMST.csv | Cached data sample (offline). | none |
| data_cache/stooq/SMX.csv | Cached data sample (offline). | none |
| data_cache/stooq/SNAG.csv | Cached data sample (offline). | none |
| data_cache/stooq/SNAL.csv | Cached data sample (offline). | none |
| data_cache/stooq/SNCY.csv | Cached data sample (offline). | none |
| data_cache/stooq/SNFCA.csv | Cached data sample (offline). | none |
| data_cache/stooq/SOFX.csv | Cached data sample (offline). | none |
| data_cache/stooq/SOUN.csv | Cached data sample (offline). | none |
| data_cache/stooq/SOUX.csv | Cached data sample (offline). | none |
| data_cache/stooq/SOXQ.csv | Cached data sample (offline). | none |
| data_cache/stooq/SOXX.csv | Cached data sample (offline). | none |
| data_cache/stooq/SPAM.csv | Cached data sample (offline). | none |
| data_cache/stooq/SPEG.csv | Cached data sample (offline). | none |
| data_cache/stooq/SPFI.csv | Cached data sample (offline). | none |
| data_cache/stooq/SPOG.csv | Cached data sample (offline). | none |
| data_cache/stooq/SRAD.csv | Cached data sample (offline). | none |
| data_cache/stooq/STAI.csv | Cached data sample (offline). | none |
| data_cache/stooq/STLD.csv | Cached data sample (offline). | none |
| data_cache/stooq/STNC.csv | Cached data sample (offline). | none |
| data_cache/stooq/STRT.csv | Cached data sample (offline). | none |
| data_cache/stooq/STRZ.csv | Cached data sample (offline). | none |
| data_cache/stooq/SUPP.csv | Cached data sample (offline). | none |
| data_cache/stooq/SUPX.csv | Cached data sample (offline). | none |
| data_cache/stooq/TALK.csv | Cached data sample (offline). | none |
| data_cache/stooq/TBMC.csv | Cached data sample (offline). | none |
| data_cache/stooq/TDWD.csv | Cached data sample (offline). | none |
| data_cache/stooq/TEM.csv | Cached data sample (offline). | none |
| data_cache/stooq/TERG.csv | Cached data sample (offline). | none |
| data_cache/stooq/TILE.csv | Cached data sample (offline). | none |
| data_cache/stooq/TLF.csv | Cached data sample (offline). | none |
| data_cache/stooq/TSDD.csv | Cached data sample (offline). | none |
| data_cache/stooq/TSEM.csv | Cached data sample (offline). | none |
| data_cache/stooq/TSL.csv | Cached data sample (offline). | none |
| data_cache/stooq/TSLG.csv | Cached data sample (offline). | none |
| data_cache/stooq/TSLL.csv | Cached data sample (offline). | none |
| docs/application_overview.md | Documentation or audit artifact. | none |
| docs/architecture.md | Documentation or audit artifact. | none |
| docs/audit/FINDINGS.md | Documentation or audit artifact. | none |
| docs/audit/REPO_AUDIT.md | Documentation or audit artifact. | none |
| docs/audit/REPO_AUDIT_UPDATE.md | Documentation or audit artifact. | none |
| docs/audit/file_manifest.json | Documentation or audit artifact. | none |
| docs/blueprint_gap_analysis.md | Documentation or audit artifact. | none |
| docs/bulk/WHERE_DATA_LIVES.md | Documentation or audit artifact. | none |
| docs/bulk_downloader.md | Documentation or audit artifact. | none |
| docs/data_provenance.md | Documentation or audit artifact. | none |
| docs/offline_watchlist_checklist.md | Documentation or audit artifact. | none |
| docs/product_roadmap.md | Documentation or audit artifact. | none |
| docs/usage.md | Documentation or audit artifact. | none |
| doctor.ps1 | PowerShell automation script. | none |
| doctor.ps1.bak_20260117_144502 | Repository metadata or auxiliary file. | none |
| doctor.ps1.bak_20260117_232037 | Repository metadata or auxiliary file. | none |
| inputs/watchlist.txt | Input watchlist or seed file. | none |
| market_app/__init__.py | Compatibility package shim. | none |
| market_monitor/__init__.py | Core pipeline module (engine). | supporting |
| market_monitor/__init__.py.bak_20260118_204345 | Core pipeline module (engine). | supporting |
| market_monitor/__main__.py | Core pipeline module (engine). | direct |
| market_monitor/bulk/__init__.py | Core pipeline module (engine). | supporting |
| market_monitor/bulk/__init__.py.bak_20260118_204345 | Core pipeline module (engine). | supporting |
| market_monitor/bulk/__init__.py.bak_20260118_210025 | Core pipeline module (engine). | supporting |
| market_monitor/bulk/downloader.py | Core pipeline module (engine). | supporting |
| market_monitor/bulk/downloader.py.bak_20260118_204345 | Core pipeline module (engine). | supporting |
| market_monitor/bulk/manifest.py | Core pipeline module (engine). | supporting |
| market_monitor/bulk/models.py | Core pipeline module (engine). | supporting |
| market_monitor/bulk/models.py.bak_20260118_204838 | Core pipeline module (engine). | supporting |
| market_monitor/bulk/planner.py | Core pipeline module (engine). | supporting |
| market_monitor/bulk/registry.py | Core pipeline module (engine). | supporting |
| market_monitor/bulk/standardize.py | Core pipeline module (engine). | supporting |
| market_monitor/cache.py | Core pipeline module (engine). | supporting |
| market_monitor/cli.py | Core pipeline module (engine). | direct |
| market_monitor/config_schema.py | Core pipeline module (engine). | supporting |
| market_monitor/corpus/__init__.py | Core pipeline module (engine). | supporting |
| market_monitor/corpus/pipeline.py | Core pipeline module (engine). | supporting |
| market_monitor/data_paths.py | Core pipeline module (engine). | supporting |
| market_monitor/doctor.py | Core pipeline module (engine). | supporting |
| market_monitor/evaluate.py | Core pipeline module (engine). | supporting |
| market_monitor/features.py | Core pipeline module (engine). | supporting |
| market_monitor/fixtures/__init__.py | Core pipeline module (engine). | supporting |
| market_monitor/fixtures/ohlcv_generator.py | Core pipeline module (engine). | supporting |
| market_monitor/gates.py | Core pipeline module (engine). | supporting |
| market_monitor/hash_utils.py | Core pipeline module (engine). | supporting |
| market_monitor/io.py | Core pipeline module (engine). | supporting |
| market_monitor/logging_utils.py | Core pipeline module (engine). | supporting |
| market_monitor/macro.py | Core pipeline module (engine). | supporting |
| market_monitor/manifest.py | Core pipeline module (engine). | supporting |
| market_monitor/offline.py | Core pipeline module (engine). | supporting |
| market_monitor/paths.py | Core pipeline module (engine). | supporting |
| market_monitor/pipeline.py | Core pipeline module (engine). | supporting |
| market_monitor/prediction.py | Core pipeline module (engine). | supporting |
| market_monitor/preflight.py | Core pipeline module (engine). | supporting |
| market_monitor/provider_factory.py | Core pipeline module (engine). | supporting |
| market_monitor/providers/__init__.py | Core pipeline module (engine). | supporting |
| market_monitor/providers/alphavantage.py | Core pipeline module (engine). | supporting |
| market_monitor/providers/base.py | Core pipeline module (engine). | supporting |
| market_monitor/providers/finnhub.py | Core pipeline module (engine). | supporting |
| market_monitor/providers/http.py | Core pipeline module (engine). | supporting |
| market_monitor/providers/nasdaq_daily.py | Core pipeline module (engine). | supporting |
| market_monitor/providers/stooq.py | Core pipeline module (engine). | supporting |
| market_monitor/providers/twelvedata.py | Core pipeline module (engine). | supporting |
| market_monitor/report.py | Core pipeline module (engine). | supporting |
| market_monitor/report.py.bak_20260118_205928 | Core pipeline module (engine). | supporting |
| market_monitor/report.py.bak_20260118_212050 | Core pipeline module (engine). | supporting |
| market_monitor/report.py.bak_20260118_212548 | Core pipeline module (engine). | supporting |
| market_monitor/risk.py | Core pipeline module (engine). | supporting |
| market_monitor/scenarios.py | Core pipeline module (engine). | supporting |
| market_monitor/scoring.py | Core pipeline module (engine). | supporting |
| market_monitor/staging.py | Core pipeline module (engine). | supporting |
| market_monitor/taxonomy/__init__.py | Core pipeline module (engine). | supporting |
| market_monitor/themes.py | Core pipeline module (engine). | supporting |
| market_monitor/universe.py | Core pipeline module (engine). | supporting |
| monitor.py | Repository metadata or auxiliary file. | direct |
| monitor_v2.py | Repository metadata or auxiliary file. | direct |
| mypy.ini | Repository metadata or auxiliary file. | none |
| output/eligible_20260111.csv | Output artifact (legacy or generated). | none |
| output/eligible_20260111_163524.csv | Output artifact (legacy or generated). | none |
| output/features_20260111.csv | Output artifact (legacy or generated). | none |
| output/features_20260111_163524.csv | Output artifact (legacy or generated). | none |
| output/scored_20260111.csv | Output artifact (legacy or generated). | none |
| output/scored_20260111_163524.csv | Output artifact (legacy or generated). | none |
| py/append_json_row.py | Utility tooling or ad-hoc helper script. | supporting |
| py/compute_features.py | Utility tooling or ad-hoc helper script. | supporting |
| py/compute_features.py.bak_20260111_011044 | Utility tooling or ad-hoc helper script. | supporting |
| py/compute_features.py.bak_20260111_131630 | Utility tooling or ad-hoc helper script. | supporting |
| py/score_security.py | Utility tooling or ad-hoc helper script. | supporting |
| py/score_security.py.bak_20260111_011044 | Utility tooling or ad-hoc helper script. | supporting |
| py/score_security.py.bak_20260111_131630 | Utility tooling or ad-hoc helper script. | supporting |
| pyproject.toml | Repository metadata or auxiliary file. | none |
| requirements.txt | Repository metadata or auxiliary file. | none |
| run.ps1 | PowerShell automation script. | direct |
| run_all.ps1 | PowerShell automation script. | direct |
| run_universe.ps1 | PowerShell automation script. | direct |
| run_universe.ps1.bak_20260111_131630 | Repository metadata or auxiliary file. | none |
| scripts/acceptance.ps1 | PowerShell automation script. | direct |
| scripts/git_hygiene_check.ps1 | PowerShell automation script. | supporting |
| scripts/run.ps1 | PowerShell automation script. | direct |
| scripts/runner.py | Automation helper script. | supporting |
| seed_watchlist.csv | Repository metadata or auxiliary file. | none |
| seed_watchlist_dedup.csv | Repository metadata or auxiliary file. | none |
| setup.ps1 | PowerShell automation script. | none |
| src/market_app/__init__.py | Blueprint wrapper module. | supporting |
| src/market_app/cli.py | Blueprint wrapper module. | direct |
| src/market_app/config.py | Blueprint wrapper module. | supporting |
| src/market_app/outputs.py | Blueprint wrapper module. | supporting |
| tests/conftest.py | Test module or test fixture data. | none |
| tests/fixtures/blueprint_config.yaml | Test module or test fixture data. | none |
| tests/fixtures/corpus/gdelt_conflict_sample.csv | Test module or test fixture data. | none |
| tests/fixtures/data/cache/nasdaq_daily/AAA.parquet | Test module or test fixture data. | none |
| tests/fixtures/data/cache/nasdaq_daily/BBB.parquet | Test module or test fixture data. | none |
| tests/fixtures/data/cache/nasdaq_daily/SPY.parquet | Test module or test fixture data. | none |
| tests/fixtures/data/state/batch_state.json | Test module or test fixture data. | none |
| tests/fixtures/data/universe_local.csv | Test module or test fixture data. | none |
| tests/fixtures/data/universe_universe.csv | Test module or test fixture data. | none |
| tests/fixtures/macro/term_spread.csv | Test module or test fixture data. | none |
| tests/fixtures/minimal_config.local.yaml | Test module or test fixture data. | none |
| tests/fixtures/minimal_config.yaml | Test module or test fixture data. | none |
| tests/fixtures/nasdaq_daily/AAA.csv | Test module or test fixture data. | none |
| tests/fixtures/nasdaq_daily/BBB.csv | Test module or test fixture data. | none |
| tests/fixtures/nasdaq_daily/BRK-B.csv | Test module or test fixture data. | none |
| tests/fixtures/nasdaq_daily/SORT.csv | Test module or test fixture data. | none |
| tests/fixtures/ohlcv.csv | Test module or test fixture data. | none |
| tests/fixtures/ohlcv/AAA.csv | Test module or test fixture data. | none |
| tests/fixtures/ohlcv/BBB.csv | Test module or test fixture data. | none |
| tests/fixtures/ohlcv/SPY.csv | Test module or test fixture data. | none |
| tests/fixtures/watchlist.txt | Test module or test fixture data. | none |
| tests/fixtures/watchlist_local.txt | Test module or test fixture data. | none |
| tests/fixtures/watchlists.yaml | Test module or test fixture data. | none |
| tests/test_bulk_manifest.py | Test module or test fixture data. | none |
| tests/test_bulk_planner.py | Test module or test fixture data. | none |
| tests/test_bulk_registry.py | Test module or test fixture data. | none |
| tests/test_bulk_standardize.py | Test module or test fixture data. | none |
| tests/test_cli_wiring.py | Test module or test fixture data. | none |
| tests/test_config_path_resolution.py | Test module or test fixture data. | none |
| tests/test_corpus_pipeline.py | Test module or test fixture data. | none |
| tests/test_doctor_evaluate.py | Test module or test fixture data. | none |
| tests/test_evaluate.py | Test module or test fixture data. | none |
| tests/test_features.py | Test module or test fixture data. | none |
| tests/test_features_golden_master.py | Test module or test fixture data. | none |
| tests/test_fixture_ohlcv.py | Test module or test fixture data. | none |
| tests/test_fixture_pipeline.py | Test module or test fixture data. | none |
| tests/test_gates.py | Test module or test fixture data. | none |
| tests/test_market_app_cli.py | Test module or test fixture data. | none |
| tests/test_nasdaq_daily_provider.py | Test module or test fixture data. | none |
| tests/test_no_network.py | Test module or test fixture data. | none |
| tests/test_offline_watchlist_socket.py | Test module or test fixture data. | none |
| tests/test_reproducibility.py | Test module or test fixture data. | none |
| tests/test_run_watchlist_features.py | Test module or test fixture data. | none |
| tests/test_scoring.py | Test module or test fixture data. | none |
| tests/test_smoke_run.py | Test module or test fixture data. | none |
| tests/test_taxonomy.py | Test module or test fixture data. | none |
| tmp_report.md | Repository metadata or auxiliary file. | none |
| tools/__init__.py | Utility tooling or ad-hoc helper script. | supporting |
| tools/run_watchlist.py | Utility tooling or ad-hoc helper script. | direct |
| watchlist.txt | Repository metadata or auxiliary file. | none |