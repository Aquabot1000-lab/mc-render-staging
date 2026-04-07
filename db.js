/**
 * Mission Control: Supabase Data Layer
 * Replaces in-memory arrays + local JSON files with Supabase persistence.
 * 
 * Drop-in replacement: all functions match the patterns used in server.js
 * so the endpoint logic stays identical.
 */

const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.MC_SUPABASE_URL || process.env.SUPABASE_URL || 'https://ylxreuqvofgbpsatfsvr.supabase.co';
const SUPABASE_KEY = process.env.MC_SUPABASE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlseHJldXF2b2ZnYnBzYXRmc3ZyIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTg1NzE4MCwiZXhwIjoyMDg3NDMzMTgwfQ.DxTv7ZC0oNRHBBS0Jxquh1M0wsGV8fQ005Q9S2iILdE';

const mcDb = createClient(SUPABASE_URL, SUPABASE_KEY);

// ========== FIELD MAPPING ==========
// Server uses camelCase, Supabase uses snake_case

function taskToRow(t) {
  return {
    id: t.id,
    company: t.company,
    category: t.category,
    title: t.title,
    detail: t.detail || '',
    owner: t.owner || 'Bot',
    trigger: t.trigger || 'Agent',
    status: t.status || 'queued',
    priority: t.priority || null,
    severity: t.severity || null,
    estimated_value: t.estimatedValue || null,
    tier: t.tier || null,
    impact: t.impact || null,
    source_url: t.sourceUrl || null,
    due_date: t.dueDate || null,
    assignee: t.assignee || null,
    agent: t.agent || null,
    current_step: t.currentStep || null,
    next_action: t.nextAction || null,
    next_action_at: t.nextActionAt || null,
    last_action_at: t.lastActionAt || null,
    error: t.error || null,
    event_type: t.eventType || null,
    event_data: t.eventData || null,
    sla: t.sla || null,
    sla_deadline: t.slaDeadline || null,
    sla_status: t.slaStatus || null,
    time_left: t.timeLeft || null,
    sla_overdue_at: t.slaOverdueAt || null,
    sla_escalated_at: t.slaEscalatedAt || null,
    bot_blocked: t.botBlocked || false,
    rolled_forward: t.rolledForward || false,
    original_priority: t.originalPriority || null,
    started_at: t.startedAt || null,
    completed_at: t.completedAt || null,
    created_at: t.createdAt || new Date().toISOString(),
    updated_at: t.updatedAt || new Date().toISOString()
  };
}

function rowToTask(r) {
  return {
    id: r.id,
    company: r.company,
    category: r.category,
    title: r.title,
    detail: r.detail,
    owner: r.owner,
    trigger: r.trigger,
    status: r.status,
    priority: r.priority,
    severity: r.severity,
    estimatedValue: r.estimated_value,
    tier: r.tier,
    impact: r.impact,
    sourceUrl: r.source_url,
    dueDate: r.due_date,
    assignee: r.assignee,
    agent: r.agent,
    currentStep: r.current_step,
    nextAction: r.next_action,
    nextActionAt: r.next_action_at,
    lastActionAt: r.last_action_at,
    error: r.error,
    eventType: r.event_type,
    eventData: r.event_data,
    sla: r.sla,
    slaDeadline: r.sla_deadline,
    slaStatus: r.sla_status,
    timeLeft: r.time_left,
    slaOverdueAt: r.sla_overdue_at,
    slaEscalatedAt: r.sla_escalated_at,
    botBlocked: r.bot_blocked,
    rolledForward: r.rolled_forward,
    originalPriority: r.original_priority,
    startedAt: r.started_at,
    completedAt: r.completed_at,
    createdAt: r.created_at,
    updatedAt: r.updated_at
  };
}

function agentToRow(a) {
  return {
    id: a.id,
    name: a.name,
    company: a.company || null,
    type: a.type || 'loop',
    description: a.description || null,
    capabilities: a.capabilities || [],
    current_task_id: a.currentTaskId || null,
    current_task: a.currentTask || null,
    error: a.error || null,
    last_action: a.lastAction || null,
    last_heartbeat: a.lastHeartbeat || null,
    status: a.status || 'idle',
    created_at: a.createdAt || new Date().toISOString(),
    updated_at: a.updatedAt || new Date().toISOString()
  };
}

function rowToAgent(r) {
  return {
    id: r.id,
    name: r.name,
    company: r.company,
    type: r.type,
    description: r.description,
    capabilities: r.capabilities,
    currentTaskId: r.current_task_id,
    currentTask: r.current_task,
    error: r.error,
    lastAction: r.last_action,
    lastHeartbeat: r.last_heartbeat,
    status: r.status,
    createdAt: r.created_at,
    updatedAt: r.updated_at
  };
}

// ========== TASK OPERATIONS ==========

async function getAllTasks(filters = {}) {
  let query = mcDb.from('mc_tasks').select('*').neq('status', 'archived').order('created_at', { ascending: false });
  
  if (filters.company) query = query.eq('company', filters.company);
  if (filters.category) query = query.eq('category', filters.category);
  if (filters.status) query = query.eq('status', filters.status);
  if (filters.owner) query = query.eq('owner', filters.owner);
  
  const { data, error } = await query;
  if (error) throw error;
  return (data || []).map(rowToTask);
}

async function getTaskById(id) {
  const { data, error } = await mcDb.from('mc_tasks').select('*').eq('id', id).single();
  if (error) {
    if (error.code === 'PGRST116') return null; // Not found
    throw error;
  }
  return rowToTask(data);
}

async function createTask(task) {
  const row = taskToRow(task);
  const { data, error } = await mcDb.from('mc_tasks').insert(row).select().single();
  if (error) throw error;
  return rowToTask(data);
}

async function updateTask(id, updates) {
  // Convert camelCase updates to snake_case
  const row = {};
  const fieldMap = {
    status: 'status', detail: 'detail', agent: 'agent', assignee: 'assignee',
    currentStep: 'current_step', nextAction: 'next_action', nextActionAt: 'next_action_at',
    lastActionAt: 'last_action_at', error: 'error', startedAt: 'started_at',
    completedAt: 'completed_at', updatedAt: 'updated_at', tier: 'tier',
    impact: 'impact', severity: 'severity', estimatedValue: 'estimated_value',
    sla: 'sla', slaDeadline: 'sla_deadline', slaStatus: 'sla_status',
    timeLeft: 'time_left', slaOverdueAt: 'sla_overdue_at', slaEscalatedAt: 'sla_escalated_at',
    botBlocked: 'bot_blocked', rolledForward: 'rolled_forward', originalPriority: 'original_priority',
    owner: 'owner', trigger: 'trigger', priority: 'priority', sourceUrl: 'source_url',
    dueDate: 'due_date', eventType: 'event_type', eventData: 'event_data',
    title: 'title', company: 'company', category: 'category'
  };
  
  for (const [key, val] of Object.entries(updates)) {
    const dbKey = fieldMap[key] || key;
    row[dbKey] = val;
  }
  row.updated_at = new Date().toISOString();
  
  const { data, error } = await mcDb.from('mc_tasks').update(row).eq('id', id).select().single();
  if (error) throw error;
  return rowToTask(data);
}

async function deleteTask(id) {
  const { data, error } = await mcDb.from('mc_tasks').delete().eq('id', id).select().single();
  if (error) throw error;
  return data ? rowToTask(data) : null;
}

async function archiveTask(id) {
  return updateTask(id, { status: 'archived', updatedAt: new Date().toISOString() });
}

async function findQueuedTask(company) {
  let query = mcDb.from('mc_tasks').select('*').eq('status', 'queued').order('created_at', { ascending: true }).limit(1);
  if (company) query = query.eq('company', company);
  
  const { data, error } = await query;
  if (error) throw error;
  return data && data.length > 0 ? rowToTask(data[0]) : null;
}

async function findTaskByAgent(agentName) {
  const { data, error } = await mcDb.from('mc_tasks')
    .select('*')
    .eq('agent', agentName)
    .eq('status', 'in_progress')
    .limit(1);
  if (error) throw error;
  return data && data.length > 0 ? rowToTask(data[0]) : null;
}

async function findTaskByTitle(title, sinceISO) {
  const { data, error } = await mcDb.from('mc_tasks')
    .select('*')
    .eq('title', title)
    .gte('created_at', sinceISO)
    .limit(1);
  if (error) throw error;
  return data && data.length > 0 ? rowToTask(data[0]) : null;
}

async function getTaskCount() {
  const { count, error } = await mcDb.from('mc_tasks').select('*', { count: 'exact', head: true });
  if (error) throw error;
  return count || 0;
}

async function getTaskStats() {
  const { data, error } = await mcDb.from('mc_tasks').select('status');
  if (error) throw error;
  
  const stats = { total: 0, queued: 0, inProgress: 0, completed: 0, failed: 0, blocked: 0 };
  for (const row of (data || [])) {
    stats.total++;
    if (row.status === 'queued' || row.status === 'pending') stats.queued++;
    else if (row.status === 'in_progress') stats.inProgress++;
    else if (row.status === 'completed') stats.completed++;
    else if (row.status === 'failed') stats.failed++;
    else if (row.status === 'blocked') stats.blocked++;
  }
  return stats;
}

// ========== AGENT OPERATIONS ==========

async function getAllAgents() {
  const { data, error } = await mcDb.from('mc_agents').select('*').order('name');
  if (error) throw error;
  return (data || []).map(rowToAgent);
}

async function getAgentByName(name) {
  const { data, error } = await mcDb.from('mc_agents').select('*').eq('name', name).single();
  if (error) {
    if (error.code === 'PGRST116') return null;
    throw error;
  }
  return rowToAgent(data);
}

async function getAgentById(id) {
  // Try by id first, then by name
  let { data, error } = await mcDb.from('mc_agents').select('*').eq('id', id).single();
  if (error && error.code === 'PGRST116') {
    ({ data, error } = await mcDb.from('mc_agents').select('*').eq('name', id).single());
  }
  if (error) {
    if (error.code === 'PGRST116') return null;
    throw error;
  }
  return rowToAgent(data);
}

async function upsertAgent(agent) {
  const row = agentToRow(agent);
  const { data, error } = await mcDb.from('mc_agents').upsert(row, { onConflict: 'name' }).select().single();
  if (error) throw error;
  return rowToAgent(data);
}

async function updateAgent(name, updates) {
  const row = {};
  const fieldMap = {
    status: 'status', lastHeartbeat: 'last_heartbeat', lastAction: 'last_action',
    currentTaskId: 'current_task_id', currentTask: 'current_task', error: 'error',
    updatedAt: 'updated_at'
  };
  
  for (const [key, val] of Object.entries(updates)) {
    const dbKey = fieldMap[key] || key;
    row[dbKey] = val;
  }
  row.updated_at = new Date().toISOString();
  
  const { data, error } = await mcDb.from('mc_agents').update(row).eq('name', name).select().single();
  if (error) throw error;
  return rowToAgent(data);
}

async function deleteAgent(idOrName) {
  // Try by id first
  let { data, error } = await mcDb.from('mc_agents').delete().eq('id', idOrName).select().single();
  if (error && error.code === 'PGRST116') {
    ({ data, error } = await mcDb.from('mc_agents').delete().eq('name', idOrName).select().single());
  }
  if (error && error.code !== 'PGRST116') throw error;
  return data ? rowToAgent(data) : null;
}

async function getAgentStats() {
  const agents = await getAllAgents();
  const now = Date.now();
  
  let working = 0, idle = 0, blocked = 0, stale = 0;
  for (const a of agents) {
    const lastHb = a.lastHeartbeat ? new Date(a.lastHeartbeat).getTime() : 0;
    const staleSec = Math.floor((now - lastHb) / 1000);
    if (staleSec > 120) { stale++; continue; }
    if (a.status === 'working') working++;
    else if (a.status === 'blocked') blocked++;
    else idle++;
  }
  
  return { total: agents.length, working, idle, blocked, stale };
}

// ========== TASK EVENTS ==========

async function addTaskEvent(taskId, eventType, detail, agent) {
  const { error } = await mcDb.from('mc_task_events').insert({
    task_id: taskId,
    event_type: eventType,
    detail: detail || null,
    agent: agent || null
  });
  if (error) console.error('Task event error:', error.message);
}

async function getTaskEvents(taskId) {
  const { data, error } = await mcDb.from('mc_task_events')
    .select('*')
    .eq('task_id', taskId)
    .order('created_at', { ascending: true });
  if (error) throw error;
  return data || [];
}

// ========== HEALTH ==========

async function healthCheck() {
  const agentStats = await getAgentStats();
  const taskStats = await getTaskStats();
  
  return {
    ok: true,
    storage: 'supabase',
    agents: agentStats,
    tasks: taskStats
  };
}

module.exports = {
  mcDb,
  // Tasks
  getAllTasks, getTaskById, createTask, updateTask, deleteTask, archiveTask,
  findQueuedTask, findTaskByAgent, findTaskByTitle, getTaskCount, getTaskStats,
  // Agents
  getAllAgents, getAgentByName, getAgentById, upsertAgent, updateAgent, deleteAgent, getAgentStats,
  // Events
  addTaskEvent, getTaskEvents,
  // Health
  healthCheck,
  // Mappers (for direct use if needed)
  taskToRow, rowToTask, agentToRow, rowToAgent
};
