let SessionLoad = 1
let s:so_save = &g:so | let s:siso_save = &g:siso | setg so=0 siso=0 | setl so=-1 siso=-1
let v:this_session=expand("<sfile>:p")
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
badd +1 term://~/Repos/newspipe/src//89645:/usr/bin/fish
badd +1 ~/Repos/newspipe/pyproject.toml
badd +1 control/main.py
badd +1 ~/Repos/infra-stuff/nvim/lua/core/keymaps.lua
badd +1 ~/Repos/cs
badd +1 ~/Repos/newspipe/input_files/relevance_policies.json
badd +13 ../input_files/traversal_policies.json
badd +1 control/dependency_layers.py
badd +89 ../newspipe.vim
badd +22 ../input_files/seed_urls.json
badd +14 ~/Repos/infra-stuff/nvim/lua/core/debuggerconfig.lua
badd +122 src/control/main.py
badd +78 src/domain/services/scraper.py
badd +11 input_files/traversal_policies.json
badd +250 src/control/dependency_layers.py
badd +77 src/domain/models.py
badd +51 ~/.cache/nvim/dap.log
badd +1 ~/.cache/nvim/dap-python-stderr.log
badd +275 Session.vim
badd +291 \[dap-terminal]\ Python:\ module\ src.control.main
badd +1 src/newspipe.log
badd +68 src/domain/services/discovery_consumer.py
badd +43 src/application/services/data_ingestion.py
badd +0 term://~/Repos/newspipe//125731:/usr/bin/fish
badd +944 ../pipeline_infra/data_platform.tf
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
edit ~/Repos/newspipe/input_files/relevance_policies.json
argglobal
balt ../input_files/traversal_policies.json
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
edit input_files/traversal_policies.json
argglobal
balt ~/Repos/newspipe/input_files/relevance_policies.json
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
let s:l = 250 - ((27 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 250
normal! 031|
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
let s:l = 122 - ((7 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 122
normal! 010|
tabnext
edit src/domain/services/discovery_consumer.py
wincmd t
let s:save_winminheight = &winminheight
let s:save_winminwidth = &winminwidth
set winminheight=0
set winheight=1
set winminwidth=0
set winwidth=1
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
let s:l = 68 - ((31 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 68
normal! 0
tabnext
edit src/application/services/data_ingestion.py
argglobal
balt src/domain/services/discovery_consumer.py
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
let s:l = 43 - ((1 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 43
normal! 0
tabnext
edit src/domain/services/scraper.py
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
let s:l = 78 - ((0 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 78
normal! 0
tabnext
edit ~/Repos/infra-stuff/nvim/lua/core/keymaps.lua
argglobal
balt application/services/data_ingestion.py
setlocal foldmethod=manual
setlocal foldexpr=v:lua.vim.treesitter.foldexpr()
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
if bufexists(fnamemodify("term://~/Repos/newspipe/src//89645:/usr/bin/fish", ":p")) | buffer term://~/Repos/newspipe/src//89645:/usr/bin/fish | else | edit term://~/Repos/newspipe/src//89645:/usr/bin/fish | endif
if &buftype ==# 'terminal'
  silent file term://~/Repos/newspipe/src//89645:/usr/bin/fish
endif
balt domain/services/scraper.py
setlocal foldmethod=manual
setlocal foldexpr=0
setlocal foldmarker={{{,}}}
setlocal foldignore=#
setlocal foldlevel=0
setlocal foldminlines=1
setlocal foldnestmax=20
setlocal foldenable
let s:l = 2494 - ((0 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 2494
normal! 035|
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
let s:l = 673 - ((31 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 673
normal! 035|
tabnext 5
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
