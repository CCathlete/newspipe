let SessionLoad = 1
let s:so_save = &g:so | let s:siso_save = &g:siso | setg so=0 siso=0 | setl so=-1 siso=-1
let v:this_session=expand("<sfile>:p")
let NvimTreeSetup =  1 
let TabbyTabNames = "{\"1\":\"main\",\"2\":\"kafka\",\"3\":\"ingestion\",\"4\":\"discovery\",\"5\":\"interfaces\",\"6\":\"litellm\",\"7\":\"debug\",\"8\":\"terraform\",\"9\":\"terminal\",\"10\":\"log\"}"
let NvimTreeRequired =  1 
silent only
silent tabonly
cd ~/Repos/newspipe
if expand('%') == '' && !&modified && line('$') <= 1 && getline(1) == ''
  let s:wipebuf = bufnr('%')
endif
let s:shortmess_save = &shortmess
if &shortmess =~ 'A'
  set shortmess=aoOA
else
  set shortmess=aoO
endif
badd +81 infrastructure/lakehouse.py
badd +55 application/services/data_ingestion.py
badd +1 infrastructure/litellm_client.py
badd +1 domain/services/scraper.py
badd +927 term://~/Repos/newspipe/src//89645:/usr/bin/fish
badd +1 pyproject.toml
badd +1 control/main.py
badd +32 ~/Repos/infra-stuff/nvim/lua/core/keymaps.lua
badd +1 ~/Repos/cs
badd +14 input_files/relevance_policies.json
badd +13 ../input_files/traversal_policies.json
badd +1 control/dependency_layers.py
badd +89 ~/Repos/newspipe.vim
badd +22 ../input_files/seed_urls.json
badd +14 ~/Repos/infra-stuff/nvim/lua/core/debuggerconfig.lua
badd +104 src/control/main.py
badd +1 src/domain/services/scraper.py
badd +13 input_files/traversal_policies.json
badd +69 src/control/dependency_layers.py
badd +117 src/domain/models.py
badd +51 ~/.cache/nvim/dap.log
badd +1 ~/.cache/nvim/dap-python-stderr.log
badd +275 Session.vim
badd +1 src/newspipe.log
badd +1 src/domain/services/discovery_consumer.py
badd +135 src/application/services/data_ingestion.py
badd +1 term://~/Repos/newspipe//125731:/usr/bin/fish
badd +697 ~/Repos/pipeline_infra/data_platform.tf
badd +1 input_files/seed_urls.json
badd +1 \[dap-repl-41]
badd +1 newspipe.log
badd +135 src/infrastructure/litellm_client.py
badd +1 ~/Repos/pipeline_infra/env.auto.tfvars
badd +200 ~/Repos/pipeline_infra/variables.tf
badd +1 \[dap-repl-48]
badd +8 src/application/services/discovery_consumer.py
badd +1 src/domain/interfaces.py
badd +5 src/infrastructure/kafka.py
badd +1 src/domain/services/data_ingestion.py
badd +1 term://~/Repos/newspipe//382906:/usr/bin/fish
badd +1 \[dap-terminal]\ dap-1
badd +1 \[dap-repl-138]
badd +1 \[dap-terminal]\ Python:\ module\ src.control.main
badd +1 \[dap-repl-66]
badd +1 notebooks/python/silver_layer.py
badd +32 term://~/Repos/newspipe//6787:/usr/bin/fish
badd +1 .gitignore
badd +1 src/infrastructure/lakehouse.py
badd +1 notebooks/python/silver_layer.ipynb
badd +1 notebooks/python/silver_layer.md
badd +1 term://~/Repos/newspipe//35919:quarto\ preview\ \'/home/kcat/Repos/newspipe/notebooks/python/silver_layer.md\'\ 
badd +1 notebooks/python/silver_layer.qmd
argglobal
%argdel
$argadd infrastructure/lakehouse.py
set stal=2
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabrewind
edit notebooks/python/silver_layer.ipynb
wincmd t
let s:save_winminheight = &winminheight
let s:save_winminwidth = &winminwidth
set winminheight=0
set winheight=1
set winminwidth=0
set winwidth=1
argglobal
balt notebooks/python/silver_layer.qmd
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
silent! normal! zE
let &fdl = &fdl
let s:l = 1 - ((0 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 1
normal! 0
tabnext
edit src/infrastructure/kafka.py
argglobal
balt src/control/main.py
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
silent! normal! zE
let &fdl = &fdl
let s:l = 5 - ((4 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 5
normal! 012|
tabnext
edit src/domain/services/data_ingestion.py
argglobal
balt src/control/dependency_layers.py
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
silent! normal! zE
let &fdl = &fdl
let s:l = 1 - ((0 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 1
normal! 0
tabnext
edit src/control/main.py
argglobal
balt src/application/services/discovery_consumer.py
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
silent! normal! zE
let &fdl = &fdl
let s:l = 104 - ((25 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 104
normal! 024|
tabnext
edit src/domain/interfaces.py
argglobal
balt src/control/main.py
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
silent! normal! zE
let &fdl = &fdl
let s:l = 1 - ((0 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 1
normal! 0
tabnext
edit src/infrastructure/litellm_client.py
argglobal
balt src/domain/interfaces.py
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
silent! normal! zE
let &fdl = &fdl
let s:l = 135 - ((25 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 135
normal! 025|
tabnext
edit \[dap-terminal]\ Python:\ module\ src.control.main
let s:save_splitbelow = &splitbelow
let s:save_splitright = &splitright
set splitbelow splitright
wincmd _ | wincmd |
split
wincmd _ | wincmd |
split
2wincmd k
wincmd w
wincmd w
let &splitbelow = s:save_splitbelow
let &splitright = s:save_splitright
wincmd t
let s:save_winminheight = &winminheight
let s:save_winminwidth = &winminwidth
set winminheight=0
set winheight=1
set winminwidth=0
set winwidth=1
wincmd =
argglobal
balt src/domain/services/scraper.py
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
silent! normal! zE
let &fdl = &fdl
let s:l = 1 - ((0 * winheight(0) + 5) / 10)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 1
normal! 0
wincmd w
argglobal
if bufexists(fnamemodify("src/domain/services/scraper.py", ":p")) | buffer src/domain/services/scraper.py | else | edit src/domain/services/scraper.py | endif
if &buftype ==# 'terminal'
  silent file src/domain/services/scraper.py
endif
balt src/domain/models.py
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
silent! normal! zE
let &fdl = &fdl
let s:l = 114 - ((2 * winheight(0) + 4) / 9)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 114
normal! 0
wincmd w
argglobal
if bufexists(fnamemodify("\[dap-repl-66]", ":p")) | buffer \[dap-repl-66] | else | edit \[dap-repl-66] | endif
if &buftype ==# 'terminal'
  silent file \[dap-repl-66]
endif
balt src/domain/services/scraper.py
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
silent! normal! zE
let &fdl = &fdl
let s:l = 1 - ((0 * winheight(0) + 4) / 9)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 1
normal! 0
wincmd w
wincmd =
tabnext
edit ~/Repos/pipeline_infra/data_platform.tf
argglobal
balt ~/Repos/pipeline_infra/variables.tf
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
silent! normal! zE
let &fdl = &fdl
let s:l = 697 - ((0 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 697
normal! 03|
tabnext
argglobal
if bufexists(fnamemodify("term://~/Repos/newspipe//382906:/usr/bin/fish", ":p")) | buffer term://~/Repos/newspipe//382906:/usr/bin/fish | else | edit term://~/Repos/newspipe//382906:/usr/bin/fish | endif
if &buftype ==# 'terminal'
  silent file term://~/Repos/newspipe//382906:/usr/bin/fish
endif
balt ~/Repos/pipeline_infra/data_platform.tf
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
let s:l = 1 - ((0 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 1
normal! 0
tabnext
argglobal
if bufexists(fnamemodify("term://~/Repos/newspipe//125731:/usr/bin/fish", ":p")) | buffer term://~/Repos/newspipe//125731:/usr/bin/fish | else | edit term://~/Repos/newspipe//125731:/usr/bin/fish | endif
if &buftype ==# 'terminal'
  silent file term://~/Repos/newspipe//125731:/usr/bin/fish
endif
balt term://~/Repos/newspipe/src//89645:/usr/bin/fish
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
let s:l = 30 - ((29 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 30
normal! 0
tabnext 1
set stal=1
if exists('s:wipebuf') && len(win_findbuf(s:wipebuf)) == 0 && getbufvar(s:wipebuf, '&buftype') isnot# 'terminal'
  silent exe 'bwipe ' . s:wipebuf
endif
unlet! s:wipebuf
set winheight=1 winwidth=20
let &shortmess = s:shortmess_save
let s:sx = expand("<sfile>:p:r")."x.vim"
if filereadable(s:sx)
  exe "source " . fnameescape(s:sx)
endif
let &g:so = s:so_save | let &g:siso = s:siso_save
set hlsearch
doautoall SessionLoadPost
unlet SessionLoad
" vim: set ft=vim :
