<script setup>
import { computed, onMounted, ref } from 'vue'
import { getJSON, postJSON, putJSON } from '../api'

const err = ref('')
const busy = ref(false)
const retentionDays = ref(30)
const ledgerCount = ref(0)
const holds = ref([])
const batches = ref([])
const batch = ref(null)

const holdForm = ref({
  scope: 'entity_key',
  stream_id: '',
  entity_key: '',
  start_ts: '',
  end_ts: '',
  reason: '',
})

const stats = computed(() => {
  const b = batch.value
  if (!b) return null
  return {
    scanned: b.scanned_count,
    within: b.report?.within_window_count ?? 0,
    deletable: b.deletable_count,
    frozen: b.frozen_retained_count,
    protected: b.cursor_protected_count,
    deleted: b.deleted_count,
    missing: b.missing_at_execute_count ?? 0,
    before: b.ledger_count_before ?? b.ledger_count_at_preview,
    after: b.ledger_count_after,
  }
})

const checks = computed(() => batch.value?.report?.checks || [])
const reconciled = computed(() =>
  checks.value.length ? checks.value.every((c) => c.ok) : null,
)
const deleteItems = computed(() =>
  (batch.value?.items || []).filter((i) => i.decision === 'delete'),
)
const frozenItems = computed(() =>
  (batch.value?.items || []).filter((i) => i.decision === 'frozen_retained'),
)
const protectedItems = computed(() =>
  (batch.value?.items || []).filter((i) => i.decision === 'cursor_protected'),
)

async function load() {
  err.value = ''
  try {
    const [ret, h, b] = await Promise.all([
      getJSON('/api/ledger/cleanup/retention'),
      getJSON('/api/legal-holds'),
      getJSON('/api/ledger/cleanup/batches'),
    ])
    retentionDays.value = ret.retention_days
    ledgerCount.value = ret.ledger_count
    holds.value = h
    batches.value = b
  } catch (e) {
    err.value = String(e.message || e)
  }
}

async function saveRetention() {
  busy.value = true
  try {
    const r = await putJSON('/api/ledger/cleanup/retention', {
      retention_days: Number(retentionDays.value),
    })
    retentionDays.value = r.retention_days
    await load()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function createHold() {
  const f = holdForm.value
  const body = { scope: f.scope, reason: f.reason }
  if (f.stream_id !== '' && f.stream_id !== null) body.stream_id = Number(f.stream_id)
  if (f.scope === 'entity_key') body.entity_key = f.entity_key
  if (f.scope === 'time_range') {
    if (f.start_ts) body.start_ts = f.start_ts
    if (f.end_ts) body.end_ts = f.end_ts
  }
  busy.value = true
  try {
    await postJSON('/api/legal-holds', body)
    holdForm.value = {
      scope: holdForm.value.scope, stream_id: '', entity_key: '', start_ts: '', end_ts: '', reason: '',
    }
    await load()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function releaseHold(h) {
  if (!confirm(`解除冻结 #${h.id}（${h.reason}）？解除后需重新预览才会进入删除清单。`)) return
  busy.value = true
  try {
    await postJSON(`/api/legal-holds/${h.id}/release`, {})
    await load()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function runPreview() {
  busy.value = true
  try {
    batch.value = await postJSON('/api/ledger/cleanup/preview', {
      retention_days: Number(retentionDays.value),
    })
    await load()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function runExecute() {
  if (!batch.value) return
  if (!confirm(`正式删除批次 #${batch.value.id} 的 ${batch.value.deletable_count} 条超窗账本行？冻结中的记录会被保留。`)) return
  busy.value = true
  try {
    batch.value = await postJSON(
      `/api/ledger/cleanup/batches/${batch.value.id}/execute`,
      {},
    )
    await load()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function openBatch(id) {
  try {
    batch.value = await getJSON(`/api/ledger/cleanup/batches/${id}`)
  } catch (e) {
    err.value = String(e.message || e)
  }
}

function holdText(h) {
  if (h.scope === 'entity_key') {
    return `实体键 ${h.entity_key}${h.stream_id ? `（流 ${h.stream_id}）` : ''}`
  }
  return `时间窗 ${h.start_ts || '-∞'} ~ ${h.end_ts || '+∞'}${h.stream_id ? `（流 ${h.stream_id}）` : ''}`
}

onMounted(load)
</script>

<template>
  <div class="page">
    <h1>账本清理与保全冻结</h1>
    <p class="muted">
      清理对象是变更账本 change_ledger。先预览、再执行；命中启用中冻结的超窗记录一律保留并单列，
      与隔离区关联的账本行（游标停点证据）排除不删。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="panel">
      <h2>保留窗</h2>
      <div class="row-actions">
        <label class="muted">保留天数</label>
        <input v-model.number="retentionDays" type="number" min="1" style="width: 6rem" />
        <button :disabled="busy" @click="saveRetention">保存</button>
        <span class="muted">当前账本总条数：<strong>{{ ledgerCount }}</strong></span>
      </div>
      <p class="muted">
        截止点 = 现在 − 保留天数；event_ts 早于截止点才是「超窗」。保存后需重新预览。
      </p>
    </div>

    <div class="panel">
      <h2>保全冻结名单</h2>
      <table>
        <thead>
          <tr><th>ID</th><th>范围</th><th>流</th><th>理由</th><th>状态</th><th>登记时间</th><th></th></tr>
        </thead>
        <tbody>
          <tr v-for="h in holds" :key="h.id">
            <td>{{ h.id }}</td>
            <td>{{ holdText(h) }}</td>
            <td>{{ h.stream_id ?? '全部' }}</td>
            <td>{{ h.reason }}</td>
            <td>
              <span class="tag" :class="h.active ? 'danger' : 'ok'">
                {{ h.active ? '生效中' : '已解除' }}
              </span>
            </td>
            <td class="muted">{{ h.created_at }}</td>
            <td>
              <button v-if="h.active" class="ghost" :disabled="busy" @click="releaseHold(h)">
                解除
              </button>
            </td>
          </tr>
          <tr v-if="!holds.length"><td colspan="7" class="muted">暂无冻结登记</td></tr>
        </tbody>
      </table>

      <h3>登记冻结</h3>
      <div class="row-actions">
        <select v-model="holdForm.scope">
          <option value="entity_key">实体键</option>
          <option value="time_range">时间范围</option>
        </select>
        <input v-model="holdForm.stream_id" type="number" min="1" placeholder="流ID（可空=全部）" style="width: 9rem" />
        <input
          v-if="holdForm.scope === 'entity_key'"
          v-model="holdForm.entity_key"
          placeholder="实体键，如 ORD-0801"
          style="width: 12rem"
        />
        <template v-else>
          <input v-model="holdForm.start_ts" placeholder="起始，如 2025-09-03T00:00:00Z" style="width: 14rem" />
          <input v-model="holdForm.end_ts" placeholder="结束（可空）" style="width: 12rem" />
        </template>
        <input v-model="holdForm.reason" placeholder="冻结理由（必填）" style="width: 16rem" />
        <button :disabled="busy" @click="createHold">登记冻结</button>
      </div>
    </div>

    <div class="panel">
      <h2>清理批次</h2>
      <div class="row-actions">
        <button :disabled="busy" @click="runPreview">① 扫描超窗并生成预览清单</button>
        <button
          class="ghost"
          :disabled="busy || !batch || batch.status !== 'preview'"
          @click="runExecute"
        >② 正式删除（按预览清单）</button>
        <span v-if="batch" class="muted">
          当前查看批次 #{{ batch.id }} · {{ batch.status }}
        </span>
      </div>

      <div v-if="batch" class="panel" style="margin-top: 1rem">
        <h3>批次 #{{ batch.id }} 对账</h3>
        <p class="muted">
          保留窗 {{ batch.retention_days }} 天 · 截止点 {{ batch.cutoff_ts }} ·
          生成于 {{ batch.created_at }}
          <span v-if="batch.executed_at"> · 执行于 {{ batch.executed_at }}</span>
        </p>
        <table>
          <thead>
            <tr><th>指标</th><th>值</th><th>说明</th></tr>
          </thead>
          <tbody>
            <tr><td>扫描账本条数</td><td>{{ stats.scanned }}</td><td class="muted">预览时账本全量</td></tr>
            <tr><td>未超窗</td><td>{{ stats.within }}</td><td class="muted">保留窗内，不动</td></tr>
            <tr><td>预览可删</td><td>{{ stats.deletable }}</td><td class="muted">超窗且无冻结/隔离关联</td></tr>
            <tr><td>因冻结保留</td><td>{{ stats.frozen }}</td><td class="muted">冻结优先，单列</td></tr>
            <tr><td>因游标证据排除</td><td>{{ stats.protected }}</td><td class="muted">仍在隔离区</td></tr>
            <tr v-if="batch.status === 'executed'">
              <td>实际删除</td><td>{{ stats.deleted }}</td><td class="muted">仅来自预览可删清单</td>
            </tr>
            <tr v-if="batch.status === 'executed'">
              <td>执行前/后账本条数</td>
              <td>{{ stats.before }} → {{ stats.after }}</td>
              <td class="muted">差值应等于实际删除数（+ 执行前新增冻结则只减不增）</td>
            </tr>
            <tr v-if="batch.status === 'executed'">
              <td>执行时新增冻结</td><td>{{ batch.newly_frozen_count }}</td>
              <td class="muted">预览后新登记并命中，执行前复核保留</td>
            </tr>
            <tr><td>生效中冻结登记数</td><td>{{ batch.active_hold_count }}</td><td class="muted">与冻结名单同源</td></tr>
          </tbody>
        </table>

        <p v-if="reconciled !== null" class="tag" :class="reconciled ? 'ok' : 'danger'">
          {{ reconciled ? '对账通过：所有校验项一致' : '对账不一致，请检查批次明细' }}
        </p>
        <table>
          <thead><tr><th>校验项</th><th>结果</th><th>明细</th></tr></thead>
          <tbody>
            <tr v-for="c in checks" :key="c.name">
              <td>{{ c.name }}</td>
              <td><span class="tag" :class="c.ok ? 'ok' : 'danger'">{{ c.ok ? '通过' : '失败' }}</span></td>
              <td class="muted">{{ c.detail }}</td>
            </tr>
          </tbody>
        </table>

        <h3>因冻结保留（{{ frozenItems.length }}）</h3>
        <table>
          <thead><tr><th>账本ID</th><th>流</th><th>seq</th><th>实体键</th><th>event_ts</th><th>命中冻结</th><th>理由</th></tr></thead>
          <tbody>
            <tr v-for="i in frozenItems" :key="i.id">
              <td>{{ i.ledger_id }}</td><td>{{ i.stream_id }}</td><td>{{ i.seq }}</td>
              <td>{{ i.entity_key }}</td><td>{{ i.event_ts }}</td>
              <td>#{{ i.hold_id }}</td><td class="muted">{{ i.hold_reason }}</td>
            </tr>
            <tr v-if="!frozenItems.length"><td colspan="7" class="muted">本批次无冻结命中</td></tr>
          </tbody>
        </table>

        <h3>预览可删（{{ deleteItems.length }}）</h3>
        <table>
          <thead><tr><th>账本ID</th><th>流</th><th>seq</th><th>实体键</th><th>event_ts</th><th>处置</th><th>执行结果</th></tr></thead>
          <tbody>
            <tr v-for="i in deleteItems" :key="i.id">
              <td>{{ i.ledger_id }}</td><td>{{ i.stream_id }}</td><td>{{ i.seq }}</td>
              <td>{{ i.entity_key }}</td><td>{{ i.event_ts }}</td>
              <td>{{ i.disposition }}</td>
              <td>
                <span v-if="i.execute_state" class="tag" :class="i.deleted ? 'ok' : 'warn'">
                  {{ i.execute_state }}
                </span>
                <span v-else class="muted">待执行</span>
              </td>
            </tr>
            <tr v-if="!deleteItems.length"><td colspan="7" class="muted">本批次无超窗可删记录</td></tr>
          </tbody>
        </table>

        <h3>因游标证据排除（{{ protectedItems.length }}）</h3>
        <table>
          <thead><tr><th>账本ID</th><th>流</th><th>seq</th><th>实体键</th><th>event_ts</th><th>说明</th></tr></thead>
          <tbody>
            <tr v-for="i in protectedItems" :key="i.id">
              <td>{{ i.ledger_id }}</td><td>{{ i.stream_id }}</td><td>{{ i.seq }}</td>
              <td>{{ i.entity_key }}</td><td>{{ i.event_ts }}</td>
              <td class="muted">{{ i.detail }}</td>
            </tr>
            <tr v-if="!protectedItems.length"><td colspan="6" class="muted">本批次无隔离区关联</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="panel">
      <h2>历史批次</h2>
      <table>
        <thead>
          <tr><th>ID</th><th>状态</th><th>保留天数</th><th>扫描</th><th>可删</th><th>冻结保留</th><th>实际删除</th><th>前→后</th><th>生成</th><th></th></tr>
        </thead>
        <tbody>
          <tr v-for="b in batches" :key="b.id">
            <td>{{ b.id }}</td>
            <td><span class="tag" :class="b.status === 'executed' ? 'ok' : 'warn'">{{ b.status }}</span></td>
            <td>{{ b.retention_days }}</td>
            <td>{{ b.scanned_count }}</td>
            <td>{{ b.deletable_count }}</td>
            <td>{{ b.frozen_retained_count }}</td>
            <td>{{ b.deleted_count }}</td>
            <td>{{ b.ledger_count_before ?? b.ledger_count_at_preview }} → {{ b.ledger_count_after ?? '-' }}</td>
            <td class="muted">{{ b.created_at }}</td>
            <td><button class="ghost" @click="openBatch(b.id)">查看</button></td>
          </tr>
          <tr v-if="!batches.length"><td colspan="10" class="muted">暂无清理批次</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
