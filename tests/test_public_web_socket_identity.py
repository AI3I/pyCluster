import shutil
import subprocess
from pathlib import Path

import pytest


def test_socket_rebinds_on_login_and_ignores_retired_messages():
    if not shutil.which('node'):
        pytest.skip('Node.js is required for browser-state validation')
    html = Path('web/public_dxweb/static/index.html').read_text(encoding='utf-8')
    connect = html.split('function connectWS() {', 1)[1].split('// ================================================================ Band', 1)[0]
    save = html.split('function saveWebSession() {', 1)[1].split('function syncQthFromProfile()', 1)[0]
    script = r'''
const assert = require('node:assert/strict');
let webToken='', webCall='', webAccess={}, webProfile={}, allSpots=[{old:true}];
let activeSpotSocket=null, spotSocketToken=null, wsRetryTimer=null;
const WEB_SESSION_KEY='session', WS_BASE_URL='wss://example.test/ws';
const sessionStorage={setItem(){},removeItem(){}};
const document={getElementById(){return {classList:{add(){},remove(){}}}}};
let rendered=0;
function renderTable(){rendered++;}
function mergeSpotBatch(){throw Error('Retired socket delivered spots');}
const timers=[];
function setTimeout(fn){timers.push(fn);return timers.length;}
function clearTimeout(){}
class WebSocket {
 constructor(url){this.url=url;this.handlers={};}
 addEventListener(name, fn){this.handlers[name]=fn;}
 close(){this.closed=true;}
}
'''
    script += 'function connectWS() {' + connect
    script += 'function saveWebSession() {' + save
    script += r'''
connectWS();
const anonymous=activeSpotSocket;
assert.equal(anonymous.url, WS_BASE_URL);
webToken='account-token';webCall='AI3I-99';saveWebSession();
assert(anonymous.closed);
assert.equal(activeSpotSocket.url, WS_BASE_URL+'?token=account-token');
assert.equal(allSpots.length,0);
anonymous.handlers.message({data:'{}'});
anonymous.handlers.close();
assert.equal(timers.length,0);
const authenticated=activeSpotSocket;
webToken='';webCall='';saveWebSession();
assert(authenticated.closed);
assert.equal(activeSpotSocket.url,WS_BASE_URL);
authenticated.handlers.message({data:'{}'});
'''
    result = subprocess.run(['node', '-'], input=script, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
