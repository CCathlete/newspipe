let SessionLoad = 1
let s:so_save = &g:so | let s:siso_save = &g:siso | setg so=0 siso=0 | setl so=-1 siso=-1
let v:this_session=expand("<sfile>:p")
let NvimTreeSetup =  1 
let TabbyTabNames = "{\"6\":\"Logs\",\"7\":\"Config\",\"1\":\"General\",\"2\":\"General2\",\"3\":\"Ingestion\",\"4\":\"Dependencies\",\"5\":\"Interfaces\"}"
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
badd +49 src/control/main.py
badd +72 src/domain/services/scraper.py
badd +13 input_files/traversal_policies.json
badd +17 src/control/dependency_layers.py
badd +50 src/domain/models.py
badd +51 ~/.cache/nvim/dap.log
badd +1 ~/.cache/nvim/dap-python-stderr.log
badd +275 Session.vim
badd +1 src/newspipe.log
badd +1 src/domain/services/discovery_consumer.py
badd +135 src/application/services/data_ingestion.py
badd +2024 term://~/Repos/newspipe//125731:/usr/bin/fish
badd +697 ~/Repos/pipeline_infra/data_platform.tf
badd +1 input_files/seed_urls.json
badd +1 \[dap-repl-41]
badd +1 newspipe.log
badd +135 src/infrastructure/litellm_client.py
badd +1 ~/Repos/pipeline_infra/env.auto.tfvars
badd +200 ~/Repos/pipeline_infra/variables.tf
badd +1 \[dap-repl-48]
badd +69 ~/Repos/newspipe/src/application/services/discovery_service.py
badd +56 src/domain/interfaces.py
badd +60 src/infrastructure/kafka.py
badd +132 src/domain/services/data_ingestion.py
badd +1 term://~/Repos/newspipe//382906:/usr/bin/fish
badd +1 \[dap-terminal]\ dap-1
badd +1 \[dap-repl-138]
badd +1 \[dap-repl-66]
badd +1 notebooks/python/silver_layer.py
badd +32 term://~/Repos/newspipe//6787:/usr/bin/fish
badd +49 .gitignore
badd +83 src/infrastructure/lakehouse.py
badd +100 notebooks/python/silver_layer.ipynb
badd +1 notebooks/python/silver_layer.md
badd +1 term://~/Repos/newspipe//35919:quarto\ preview\ \'/home/kcat/Repos/newspipe/notebooks/python/silver_layer.md\'\ 
badd +1 notebooks/python/silver_layer.qmd
badd +176 ~/Repos/infra-stuff/nvim/lua/plugins/molten.lua
badd +79 ~/Repos/infra-stuff/nvim/lua/plugins/lsp.lua
badd +32 term://~/Repos/newspipe//17173:/usr/bin/fish
badd +1 \[dap-repl-77]
badd +51 src/application/services/ingestion_service.py
badd +1 term
badd +1 term://~/Repos/newspipe//147748:/usr/bin/fish
badd +1 term://~/Repos/newspipe//148038:/usr/bin/fish
badd +10005 \[dap-terminal]\ Python:\ module\ src.control.main
badd +1 src/control/discovery_controller.py
badd +38 src/control/ingestion_controller.py
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
tabrewind
edit src/control/main.py
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
let s:l = 49 - ((19 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 49
normal! 066|
tabnext
edit src/control/ingestion_controller.py
wincmd t
let s:save_winminheight = &winminheight
let s:save_winminwidth = &winminwidth
set winminheight=0
set winheight=1
set winminwidth=0
set winwidth=1
argglobal
balt src/control/discovery_controller.py
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
let s:l = 38 - ((29 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 38
normal! 037|
tabnext
edit ~/Repos/newspipe/src/application/services/discovery_service.py
let s:save_splitbelow = &splitbelow
let s:save_splitright = &splitright
set splitbelow splitright
wincmd _ | wincmd |
vsplit
1wincmd h
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
balt src/application/services/ingestion_service.py
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
let s:l = 69 - ((2 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 69
normal! 0
wincmd w
argglobal
if bufexists(fnamemodify("src/application/services/ingestion_service.py", ":p")) | buffer src/application/services/ingestion_service.py | else | edit src/application/services/ingestion_service.py | endif
if &buftype ==# 'terminal'
  silent file src/application/services/ingestion_service.py
endif
balt ~/Repos/newspipe/src/application/services/discovery_service.py
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
let s:l = 89 - ((27 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 89
normal! 0
wincmd w
wincmd =
tabnext
edit src/control/dependency_layers.py
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
let s:l = 17 - ((16 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 17
normal! 023|
tabnext
edit src/infrastructure/kafka.py
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
let s:l = 60 - ((15 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 60
normal! 024|
tabnext
argglobal
if bufexists(fnamemodify("term://~/Repos/newspipe//148038:/usr/bin/fish", ":p")) | buffer term://~/Repos/newspipe//148038:/usr/bin/fish | else | edit term://~/Repos/newspipe//148038:/usr/bin/fish | endif
if &buftype ==# 'terminal'
  silent file term://~/Repos/newspipe//148038:/usr/bin/fish
endif
balt term://~/Repos/newspipe//125731:/usr/bin/fish
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
normal! 035|
tabnext
edit input_files/seed_urls.json
let s:save_splitbelow = &splitbelow
let s:save_splitright = &splitright
set splitbelow splitright
wincmd _ | wincmd |
vsplit
wincmd _ | wincmd |
vsplit
2wincmd h
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
balt input_files/relevance_policies.json
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
let s:l = 19 - ((15 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 19
normal! 016|
wincmd w
argglobal
if bufexists(fnamemodify("input_files/relevance_policies.json", ":p")) | buffer input_files/relevance_policies.json | else | edit input_files/relevance_policies.json | endif
if &buftype ==# 'terminal'
  silent file input_files/relevance_policies.json
endif
balt input_files/seed_urls.json
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
let s:l = 6 - ((4 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 6
normal! 0
wincmd w
argglobal
if bufexists(fnamemodify("input_files/traversal_policies.json", ":p")) | buffer input_files/traversal_policies.json | else | edit input_files/traversal_policies.json | endif
if &buftype ==# 'terminal'
  silent file input_files/traversal_policies.json
endif
balt input_files/relevance_policies.json
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
let s:l = 6 - ((4 * winheight(0) + 15) / 30)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 6
normal! 014|
wincmd w
wincmd =
tabnext 2
set stal=1
if exists('s:wipebuf') && len(win_findbuf(s:wipebuf)) == 0 && getbufvar(s:wipebuf, '&buftype') isnot# 'terminal'
  silent exe 'bwipe ' . s:wipebuf
endif
unlet! s:wipebuf
set winheight=1 winwidth=20
let &shortmess = s:shortmess_save
let &winminheight = s:save_winminheight
let &winminwidth = s:save_winminwidth
let s:sx = expand("<sfile>:p:r")."x.vim"
if filereadable(s:sx)
  exe "source " . fnameescape(s:sx)
endif
let &g:so = s:so_save | let &g:siso = s:siso_save
set hlsearch
doautoall SessionLoadPost
unlet SessionLoad
" vim: set ft=vim :
