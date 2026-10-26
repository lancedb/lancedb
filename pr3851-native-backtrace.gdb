set pagination off
set debuginfod enabled off
set print frame-arguments none
info threads
thread apply all bt 24
bt 48
info registers rip rsp
info proc mappings
