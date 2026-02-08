let SessionLoad = 1
let s:so_save = &g:so | let s:siso_save = &g:siso | setg so=0 siso=0 | setl so=-1 siso=-1
let v:this_session=expand("<sfile>:p")
let NvimTreeSetup =  1 
let TabbyTabNames = "{\"1\":\"ingestion\",\"2\":\"dependencies\",\"3\":\"log\"}"
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
badd +97 src/control/main.py
badd +116 src/domain/services/scraper.py
badd +13 input_files/traversal_policies.json
badd +138 src/control/dependency_layers.py
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
badd +2 src/infrastructure/kafka.py
badd +1 src/domain/services/data_ingestion.py
badd +1 term://~/Repos/newspipe//382906:/usr/bin/fish
badd +1 \[dap-terminal]\ dap-1
badd +1 \[dap-repl-138]
badd +1 \[dap-terminal]\ Python:\ module\ src.control.main
badd +1 \[dap-repl-66]
badd +1 notebooks/python/silver_layer.py
badd +32 term://~/Repos/newspipe//6787:/usr/bin/fish
badd +49 .gitignore
badd +1 src/infrastructure/lakehouse.py
badd +90 notebooks/python/silver_layer.ipynb
badd +1 notebooks/python/silver_layer.md
badd +1 term://~/Repos/newspipe//35919:quarto\ preview\ \'/home/kcat/Repos/newspipe/notebooks/python/silver_layer.md\'\ 
badd +1 notebooks/python/silver_layer.qmd
badd +176 ~/Repos/infra-stuff/nvim/lua/plugins/molten.lua
badd +79 ~/Repos/infra-stuff/nvim/lua/plugins/lsp.lua
badd +429 term://~/Repos/newspipe//17173:/usr/bin/fish
argglobal
%argdel
$argadd infrastructure/lakehouse.py
set stal=2
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabnew +setlocal\ bufhidden=wipe
tabrewind
edit notebooks/python/silver_layer.ipynb
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
balt notebooks/python/silver_layer.py
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
let s:l = 100 - ((20 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 100
normal! 041|
wincmd w
argglobal
if bufexists(fnamemodify("term://~/Repos/newspipe//17173:/usr/bin/fish", ":p")) | buffer term://~/Repos/newspipe//17173:/usr/bin/fish | else | edit term://~/Repos/newspipe//17173:/usr/bin/fish | endif
if &buftype ==# 'terminal'
  silent file term://~/Repos/newspipe//17173:/usr/bin/fish
endif
balt notebooks/python/silver_layer.ipynb
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
let s:l = 422 - ((24 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 422
normal! 09|
wincmd w
wincmd =
tabnext
edit src/control/main.py
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
let s:l = 97 - ((31 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 97
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
let s:l = 451 - ((31 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 451
normal! 035|
tabnext
edit .gitignore
argglobal
balt term://~/Repos/newspipe//125731:/usr/bin/fish
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
let s:l = 49 - ((30 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 49
normal! 02|
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
