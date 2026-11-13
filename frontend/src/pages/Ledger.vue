<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const rows = ref([])
const stats = ref(null)
const latest = ref(null)
onMounted(async () => {
  rows.value = await getJSON('/api/ledger')
  stats.value = await getJSON('/api/ledger/stats')
  const batches = await getJSON('/api/ledger/cleanup/batches')
  latest.value = batches[0] || null
})
</script>
<template>
  <div class="page">
    <h1>变更账本</h1>
    <p class="muted">每次 Apply 写入的事件处置记录（与报告同源）。</p>
    <div class="panel">
      <div class="row-actions">
        <span class="muted">账本条数：<strong>{{ stats?.ledger_count ?? '-' }}</strong></span>
        <span v-if="latest" class="muted">
          最近清理批次 #{{ latest.id }}（{{ latest.status }}）：扫描 {{ latest.scanned_count }} ·
          可删 {{ latest.deletable_count }} · 因冻结保留 {{ latest.frozen_retained_count }} ·
          实际删除 {{ latest.deleted_count }}
        </span>
        <router-link to="/cleanup">去账本清理 / 保全冻结 →</router-link>
      </div>
    </div>
    <div class="panel">
      <table>
        <thead>
          <tr><th>ID</th><th>流</th><th>run</th><th>seq</th><th>op</th><th>entity</th><th>disposition</th><th>event_ts</th></tr>
        </thead>
        <tbody>
          <tr v-for="r in rows" :key="r.id">
            <td>{{ r.id }}</td>
            <td>{{ r.stream_id }}</td>
            <td>{{ r.run_id }}</td>
            <td>{{ r.seq }}</td>
            <td>{{ r.op }}</td>
            <td>{{ r.entity_key }}</td>
            <td>{{ r.disposition }}</td>
            <td>{{ r.event_ts }}</td>
          </tr>
          <tr v-if="!rows.length"><td colspan="8" class="muted">暂无账本记录</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
