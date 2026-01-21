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
badd +27 ~/Repos/newspipe/pyproject.toml
badd +1 control/main.py
badd +1 ~/Repos/infra-stuff/nvim/lua/core/keymaps.lua
badd +1 ~/Repos/cs
badd +14 ~/Repos/newspipe/input_files/relevance_policies.json
badd +13 ../input_files/traversal_policies.json
badd +1 control/dependency_layers.py
badd +89 ../newspipe.vim
badd +22 ../input_files/seed_urls.json
badd +1 ~/Repos/infra-stuff/nvim/lua/core/debuggerconfig.lua
badd +1 src/control/main.py
badd +1 src/domain/services/scraper.py
badd +0 input_files/traversal_policies.json
badd +1 src/control/dependency_layers.py
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
balt input_files/traversal_policies.json
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
edit src/domain/services/scraper.py
argglobal
balt domain/services/scraper.py
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
edit src/control/main.py
argglobal
balt control/main.py
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
edit ~/Repos/infra-stuff/nvim/lua/core/debuggerconfig.lua
argglobal
balt ../input_files/seed_urls.json
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
let s:l = 2 - ((1 * winheight(0) + 16) / 32)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 2
normal! 035|
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
nohlsearch
doautoall SessionLoadPost
unlet SessionLoad
" vim: set ft=vim :
