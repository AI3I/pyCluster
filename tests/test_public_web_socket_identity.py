import shutil
import subprocess
from pathlib import Path

import pytest


def test_rbn_display_filter_uses_classification_not_comment():
    if not shutil.which('node'):
        pytest.skip('Node.js is required for browser-state validation')
    html = Path('web/public_dxweb/static/index.html').read_text(encoding='utf-8')
    function = 'function spotMatchesCurrentFilters' + html.split('function spotMatchesCurrentFilters', 1)[1].split('\n}', 1)[0] + '\n}'
    script = '''
const assert = require('node:assert/strict');
const filters={mode:'ALL',activity:'ALL',cont:'ALL',spotterCont:'ALL'};
let commentTagFilter='ALL',cqzFilter='',spotterCqzFilter='',searchTerm='';
const selectedBands=()=>[], parseZoneSpec=()=>new Set();
const COMMENT_TAGS=[];
''' + function + '''
const rbn={is_rbn:true,comment:'CW 12 dB'};
const cluster={is_rbn:false,comment:'Talking about RBN'};
assert(spotMatchesCurrentFilters(rbn));assert(spotMatchesCurrentFilters(cluster));
commentTagFilter='RBN';
assert(spotMatchesCurrentFilters(rbn));assert(!spotMatchesCurrentFilters(cluster));
commentTagFilter='NO_RBN';
assert(!spotMatchesCurrentFilters(rbn));assert(spotMatchesCurrentFilters(cluster));
'''
    result = subprocess.run(['node', '-'], input=script, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_spot_json_export_is_filtered_and_excludes_login_state():
    if not shutil.which('node'):
        pytest.skip('Node.js is required for browser-state validation')
    html = Path('web/public_dxweb/static/index.html').read_text(encoding='utf-8')
    assert 'let timeRangeHrs = 1;' in html
    assert 'class="sb-time active" data-hrs="1"' in html
    function = 'function spotDiagnosticExport() {' + html.split('function spotDiagnosticExport() {', 1)[1].split('\n}', 1)[0] + '\n}'
    script = '''
const assert = require('node:assert/strict');
const filteredSpots=()=>[{dx_call:'AI3I-99',is_rbn:false}];
const brandingData={node_call:'AI3I-90',software_version:'1.0.21'};
const filters={mode:'ALL'}, commentTagFilter='NO_RBN',cqzFilter='',spotterCqzFilter='',timeRangeHrs=1,searchTerm='';
const webToken='secret',webProfile={email:'private@example.test'};
''' + function + '''
const result=spotDiagnosticExport();
assert.equal(result.count,1);assert.equal(result.spots[0].dx_call,'AI3I-99');
assert.equal(result.filters.timeRangeHrs,1);assert.equal(result.filters.commentTagFilter,'NO_RBN');
assert(!JSON.stringify(result).includes('secret'));assert(!JSON.stringify(result).includes('private@example.test'));
assert.equal(result.format_version,1);
'''
    result = subprocess.run(['node', '-'], input=script, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


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
