<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const stats = ref(null)
const err = ref('')
onMounted(async () => {
  try {
    stats.value = await getJSON('/api/dashboard')
  } catch (e) {
    err.value = String(e.message || e)
  }
})
</script>
<template>
  <div class="page">
    <header>
      <h1>变流总览</h1>
      <p class="muted">变更事件入账 → 序/缺口裁决 → 幂等应用到 Sink → 游标对账</p>
    </header>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="tiles" v-if="stats">
      <div class="tile"><div class="hero-num">{{ stats.stream_count }}</div><div class="muted">变更流</div></div>
      <div class="tile"><div class="hero-num">{{ stats.enabled_streams }}</div><div class="muted">启用</div></div>
      <div class="tile"><div class="hero-num">{{ stats.sink_count }}</div><div class="muted">Sink</div></div>
      <div class="tile"><div class="hero-num">{{ stats.quarantine_count }}</div><div class="muted">隔离</div></div>
      <div class="tile"><div class="hero-num">{{ stats.ledger_count }}</div><div class="muted">账本（保留 {{ stats.retention_days }} 天）</div></div>
      <div class="tile"><div class="hero-num">{{ stats.active_hold_count }}</div><div class="muted">生效冻结</div></div>
      <div class="tile"><div class="hero-num">{{ stats.run_count }}</div><div class="muted">Apply Runs</div></div>
    </div>
    <p v-if="stats.latest_cleanup" class="muted">
      最近清理批次 #{{ stats.latest_cleanup.id }}（{{ stats.latest_cleanup.status }}）：
      扫描 {{ stats.latest_cleanup.scanned_count }} · 可删 {{ stats.latest_cleanup.deletable_count }} ·
      因冻结保留 {{ stats.latest_cleanup.frozen_retained_count }} ·
      实际删除 {{ stats.latest_cleanup.deleted_count }}
    </p>
    <router-link to="/cleanup">去账本清理 / 保全冻结 →</router-link>
    <br />
    <router-link to="/apply">去应用台跑一趟 →</router-link>
  </div>
</template>
