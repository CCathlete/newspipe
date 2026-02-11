let SessionLoad = 1
let s:so_save = &g:so | let s:siso_save = &g:siso | setg so=0 siso=0 | setl so=-1 siso=-1
let v:this_session=expand("<sfile>:p")
let NvimTreeSetup =  1 
let TabbyTabNames = "{\"6\":\"Config\",\"1\":\"General\",\"2\":\"Ingestion\",\"3\":\"Interfaces\",\"4\":\"Dependencies\",\"5\":\"Logs\"}"
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
badd +52 src/control/main.py
badd +86 src/domain/services/scraper.py
badd +13 input_files/traversal_policies.json
badd +26 src/control/dependency_layers.py
badd +50 src/domain/models.py
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
badd +112 src/application/services/discovery_consumer.py
badd +36 src/domain/interfaces.py
badd +2 src/infrastructure/kafka.py
badd +37 src/domain/services/data_ingestion.py
badd +1 term://~/Repos/newspipe//382906:/usr/bin/fish
badd +1 \[dap-terminal]\ dap-1
badd +1 \[dap-repl-138]
badd +1 \[dap-repl-66]
badd +1 notebooks/python/silver_layer.py
badd +32 term://~/Repos/newspipe//6787:/usr/bin/fish
badd +49 .gitignore
badd +77 src/infrastructure/lakehouse.py
badd +100 notebooks/python/silver_layer.ipynb
badd +1 notebooks/python/silver_layer.md
badd +1 term://~/Repos/newspipe//35919:quarto\ preview\ \'/home/kcat/Repos/newspipe/notebooks/python/silver_layer.md\'\ 
badd +1 notebooks/python/silver_layer.qmd
badd +176 ~/Repos/infra-stuff/nvim/lua/plugins/molten.lua
badd +79 ~/Repos/infra-stuff/nvim/lua/plugins/lsp.lua
badd +32 term://~/Repos/newspipe//17173:/usr/bin/fish
badd +0 \[dap-terminal]\ Python:\ module\ src.control.main
argglobal
%argdel
$argadd infrastructure/lakehouse.py
set stal=2
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabrewind
edit src/application/services/discovery_consumer.py
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
let s:l = 11 - ((10 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 11
normal! 012|
tabnext
edit src/application/services/discovery_consumer.py
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
exe '1resize ' . ((&lines * 16 + 17) / 35)
exe '2resize ' . ((&lines * 13 + 17) / 35)
exe '3resize ' . ((&lines * 1 + 17) / 35)
argglobal
if bufexists(fnamemodify("\[dap-terminal]\ Python:\ module\ src.control.main", ":p")) | buffer \[dap-terminal]\ Python:\ module\ src.control.main | else | edit \[dap-terminal]\ Python:\ module\ src.control.main | endif
if &buftype ==# 'terminal'
  silent file \[dap-terminal]\ Python:\ module\ src.control.main
endif
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
let s:l = 10016 - ((15 * winheight(0) + 8) / 16)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 10016
normal! 0
wincmd w
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
let s:l = 112 - ((9 * winheight(0) + 6) / 13)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 112
normal! 0122|
wincmd w
argglobal
enew
file \[dap-repl-77]
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
wincmd w
exe '1resize ' . ((&lines * 16 + 17) / 35)
exe '2resize ' . ((&lines * 13 + 17) / 35)
exe '3resize ' . ((&lines * 1 + 17) / 35)
tabnext
edit src/domain/interfaces.py
argglobal
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
let s:l = 36 - ((11 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 36
normal! 016|
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
let s:l = 1 - ((0 * winheight(0) + 16) / 32)
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
let s:l = 1221 - ((1 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 1221
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
let s:l = 19 - ((18 * winheight(0) + 16) / 32)
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
let s:l = 6 - ((5 * winheight(0) + 16) / 32)
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
let s:l = 6 - ((5 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 6
normal! 014|
wincmd w
wincmd =
tabnext 6
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
nohlsearch
doautoall SessionLoadPost
unlet SessionLoad
" vim: set ft=vim :
