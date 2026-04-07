const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs').promises;
const path = require('path');
const db = require('./db');

const app = express();
const PORT = process.env.PORT || 3003;

// Middleware
app.use(express.json());
app.use(express.static('public'));

// CORS
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, x-api-key');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// Data directory (for creators and activityLog only)
const DATA_DIR = path.join(__dirname, 'data');
const ACTIVITY_FILE = path.join(DATA_DIR, 'activity.json');
const CREATORS_FILE = path.join(DATA_DIR, 'creators.json');

// In-memory storage (creators and activityLog only - tasks/agents now in Supabase)
let activityLog = [];
let creators = [];

// Track server uptime
const startTime = Date.now();

// Initialize data directory and load persisted data (creators and activityLog only)
async function initDataStore() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });

    // Load activity log (legacy compat)
    try {
      const activityData = await fs.readFile(ACTIVITY_FILE, 'utf8');
      activityLog = JSON.parse(activityData);
      console.log(`📊 Loaded ${activityLog.length} activity entries`);
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
      activityLog = [];
    }

    // Load creators
    try {
      const creatorsData = await fs.readFile(CREATORS_FILE, 'utf8');
      creators = JSON.parse(creatorsData);
      console.log(`👥 Loaded ${creators.length} creators`);
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
      creators = [];
    }

    // Tasks and agents are now in Supabase
    const taskCount = await db.getTaskCount();
    const agents = await db.getAllAgents();
    console.log(`📋 Supabase tasks: ${taskCount}`);
    console.log(`🤖 Supabase agents: ${agents.length}`);
  } catch (err) {
    console.error('Failed to initialize data store:', err);
  }
}

// Persist data to disk (creators and activityLog only - tasks/agents auto-persist in Supabase)
async function persistData() {
  try {
    await fs.writeFile(ACTIVITY_FILE, JSON.stringify(activityLog, null, 2));
    await fs.writeFile(CREATORS_FILE, JSON.stringify(creators, null, 2));
  } catch (err) {
    console.error('Failed to persist data:', err);
  }
}

// Periodic flush (every 5 minutes)
setInterval(persistData, 5 * 60 * 1000);

// Run creator follow-up engine every 2 hours
setInterval(() => {
  const changes = runCreatorFollowUp();
  if (changes > 0) {
    console.log(`👥 Creator follow-up: ${changes} changes made`);
    persistData();
  }
}, 2 * 60 * 60 * 1000);

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n📥 Shutting down gracefully (SIGINT)...');
  await persistData();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n📥 Shutting down gracefully (SIGTERM)...');
  await persistData();
  process.exit(0);
});

// Supabase clients (KEPT EXACTLY AS-IS)
const oaClient = createClient(
  'https://ylxreuqvofgbpsatfsvr.supabase.co',
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlseHJldXF2b2ZnYnBzYXRmc3ZyIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTg1NzE4MCwiZXhwIjoyMDg3NDMzMTgwfQ.DxTv7ZC0oNRHBBS0Jxquh1M0wsGV8fQ005Q9S2iILdE'
);

const mpClient = createClient(
  'https://sxgvtocpgdpbxodzkdmt.supabase.co',
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4Z3Z0b2NwZ2RwYnhvZHprZG10Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjE5ODgxOCwiZXhwIjoyMDg3Nzc0ODE4fQ.Tir6ijmZ0S5hsZEJ-Kwu7YouGr4SNYthhu3BX-I7L6E'
);

// Fetch OA (OverAssessed) data
async function fetchOAData() {
  try {
    const { data: clients, error: clientError } = await oaClient
      .from('clients')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(50);

    if (clientError) throw clientError;

    const { data: submissions, error: subError } = await oaClient
      .from('submissions')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(50);

    if (subError) throw subError;

    return { clients: clients || [], submissions: submissions || [] };
  } catch (err) {
    console.error('OA fetch error:', err);
    return { error: err.message };
  }
}

// Fetch WA (Worthey Aquatics) data (KEPT EXACTLY AS-IS)
async function fetchWAData() {
  try {
    const headers = {
      'x-api-key': 'wb-aquabot-2026-secret',
      'Content-Type': 'application/json'
    };

    const [leadsRes, pipelineRes] = await Promise.all([
      fetch('https://wortheyflow-production.up.railway.app/api/bot/leads', { headers }),
      fetch('https://wortheyflow-production.up.railway.app/api/bot/pipeline', { headers })
    ]);

    const leads = leadsRes.ok ? await leadsRes.json() : null;
    const pipeline = pipelineRes.ok ? await pipelineRes.json() : null;

    return {
      leads: leads || [],
      pipeline: pipeline || {},
      leadCount: leads?.length || 0
    };
  } catch (err) {
    console.error('WA fetch error:', err);
    return { error: err.message };
  }
}

// Fetch MP (MilePilot) data
async function fetchMPData() {
  try {
    const { data: profiles, error: profileError } = await mpClient
      .from('profiles')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(50);

    if (profileError) throw profileError;

    const { data: trips, error: tripError } = await mpClient
      .from('trips')
      .select('*')
      .order('created_at', { ascending: false })
      .limit(50);

    if (tripError) throw tripError;

    return {
      profiles: profiles || [],
      trips: trips || [],
      userCount: profiles?.length || 0,
      tripCount: trips?.length || 0
    };
  } catch (err) {
    console.error('MP fetch error:', err);
    return { error: err.message };
  }
}

// Parse dollar value from string
function parseEstimatedValue(estimatedValue) {
  if (!estimatedValue) return 0;
  if (typeof estimatedValue === 'number') return estimatedValue;

  const str = estimatedValue.toString().toLowerCase();

  // Extract number
  const numMatch = str.match(/[\d,]+/);
  if (!numMatch) return 0;

  let value = parseInt(numMatch[0].replace(/,/g, ''));

  // Handle /yr or /mo suffix
  if (str.includes('/yr')) {
    // Already annual, no change
  } else if (str.includes('/mo')) {
    value *= 12; // Monthly to annual
  }

  return value;
}

// Calculate tier from estimated value
function calculateTier(estimatedValue) {
  const value = parseEstimatedValue(estimatedValue);
  if (value >= 10000) return 'HIGH';
  if (value >= 1000) return 'MEDIUM';
  return 'LOW';
}

// Determine impact label
function determineImpact(category, title, detail) {
  const text = `${title} ${detail}`.toLowerCase();

  if (category === 'revenue' || text.includes('revenue') || text.includes('payment') || text.includes('invoice')) {
    return 'REVENUE';
  }
  if (text.includes('lead') || text.includes('prospect') || text.includes('inbox')) {
    return 'LEADS';
  }
  if (text.includes('data') || text.includes('api') || text.includes('database')) {
    return 'DATA';
  }
  return 'OPERATIONS';
}

// Auto-detect severity for blockers
function determineSeverity(impact, title, detail) {
  const text = `${title} ${detail}`.toLowerCase();

  if (impact === 'LEADS' || impact === 'REVENUE') return 'HIGH';
  if (text.includes('api connection') || text.includes('failed') || text.includes('broken')) return 'HIGH';
  if (text.includes('urgent') || text.includes('critical')) return 'HIGH';
  return 'MEDIUM';
}

// Generate unique ID
function generateId() {
  return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
}

// ========== CREATOR PIPELINE FUNCTIONS ==========

// Auto follow-up engine for creators
async function runCreatorFollowUp() {
  const now = new Date();
  let changesCount = 0;

  for (const creator of creators) {
    const lastContact = new Date(creator.lastContactDate);
    const hoursSinceContact = (now - lastContact) / (60 * 60 * 1000);
    const daysSinceContact = hoursSinceContact / 24;

    // Stage: outreach_sent
    if (creator.stage === 'outreach_sent') {
      if (!creator.repliedAt) {
        if (daysSinceContact >= 7) {
          // 7 days no reply → move to cold
          creator.stage = 'cold';
          creator.nextAction = 'Moved to cold — no reply after 7 days';
          creator.updatedAt = now.toISOString();
          changesCount++;
        } else if (daysSinceContact >= 5) {
          // 5 days no reply → final message
          creator.nextAction = 'Send final message';
          creator.nextActionDate = now.toISOString();
          creator.updatedAt = now.toISOString();
        } else if (hoursSinceContact >= 48) {
          // 48h no reply → send follow-up
          creator.nextAction = 'Send follow-up';
          creator.nextActionDate = now.toISOString();
          creator.updatedAt = now.toISOString();
        }
      }
    }

    // Stage: replied
    if (creator.stage === 'replied') {
      const hoursSinceStatusChange = (now - new Date(creator.updatedAt)) / (60 * 60 * 1000);
      if (hoursSinceStatusChange >= 24) {
        creator.nextAction = 'Needs Tyler review';
        creator.nextActionDate = now.toISOString();
      }
    }

    // Stage: interested → auto-create task
    if (creator.stage === 'interested' && !creator.taskCreated) {
      const taskSla = 'TODAY';
      const nowISO = now.toISOString();
      const slaDeadline = calculateSlaDeadline(taskSla, nowISO);
      const slaStatus = calculateSlaStatus(slaDeadline, nowISO, taskSla);
      const timeLeft = formatTimeLeft(slaDeadline, slaStatus);

      const task = {
        id: generateId(),
        company: 'OA',
        category: 'marketing',
        title: `OA | Creator ${creator.name} interested — negotiate terms`,
        detail: `Platform: ${creator.platform}\nHandle: ${creator.handle}\nAudience: ${creator.audienceSize?.toLocaleString() || 'Unknown'}\nNiche: ${creator.niche}`,
        owner: 'Tyler',
        trigger: 'Creator Pipeline',
        status: 'needs_approval',
        estimatedValue: null,
        tier: 'MEDIUM',
        impact: 'LEADS',
        severity: null,
        sourceUrl: null,
        dueDate: null,
        assignee: 'Tyler',
        lastActionAt: nowISO,
        nextAction: 'Review and negotiate',
        nextActionAt: nowISO,
        createdAt: nowISO,
        updatedAt: nowISO,
        completedAt: null,
        sla: taskSla,
        slaDeadline,
        slaStatus,
        timeLeft,
        slaOverdueAt: null,
        slaEscalatedAt: null,
        botBlocked: false,
        rolledForward: false,
        originalPriority: null
      };

      const createdTask = await db.createTask(task);
      creator.taskCreated = true;
      creator.taskId = createdTask.id;
      creator.updatedAt = nowISO;
      changesCount++;
    }

    // Stage: committed → auto-create onboarding task
    if (creator.stage === 'committed' && !creator.onboardingTaskCreated) {
      const taskSla = '24H';
      const nowISO = now.toISOString();
      const slaDeadline = calculateSlaDeadline(taskSla, nowISO);
      const slaStatus = calculateSlaStatus(slaDeadline, nowISO, taskSla);
      const timeLeft = formatTimeLeft(slaDeadline, slaStatus);

      const task = {
        id: generateId(),
        company: 'OA',
        category: 'operations',
        title: `OA | Onboard creator ${creator.name} — send materials`,
        detail: `Platform: ${creator.platform}\nHandle: ${creator.handle}\nAudience: ${creator.audienceSize?.toLocaleString() || 'Unknown'}`,
        owner: 'Tyler',
        trigger: 'Creator Pipeline',
        status: 'pending',
        estimatedValue: null,
        tier: 'MEDIUM',
        impact: 'OPERATIONS',
        severity: null,
        sourceUrl: null,
        dueDate: null,
        assignee: 'Tyler',
        lastActionAt: nowISO,
        nextAction: 'Send onboarding materials',
        nextActionAt: nowISO,
        createdAt: nowISO,
        updatedAt: nowISO,
        completedAt: null,
        sla: taskSla,
        slaDeadline,
        slaStatus,
        timeLeft,
        slaOverdueAt: null,
        slaEscalatedAt: null,
        botBlocked: false,
        rolledForward: false,
        originalPriority: null
      };

      const createdTask = await db.createTask(task);
      creator.onboardingTaskCreated = true;
      creator.onboardingTaskId = createdTask.id;
      creator.updatedAt = nowISO;
      changesCount++;
    }

    // Check if onboarding task completed → move to active
    if (creator.stage === 'committed' && creator.onboardingTaskId) {
      const onboardingTask = await db.getTaskById(creator.onboardingTaskId);
      if (onboardingTask && onboardingTask.status === 'completed') {
        creator.stage = 'active';
        creator.nextAction = 'Creator is active';
        creator.updatedAt = now.toISOString();
        changesCount++;
      }
    }
  }

  return changesCount;
}

// ========== SLA FUNCTIONS ==========

// Calculate SLA deadline based on sla type and createdAt
function calculateSlaDeadline(sla, createdAt) {
  const created = new Date(createdAt);

  switch (sla) {
    case 'NOW':
      // 1 hour from creation
      return new Date(created.getTime() + 60 * 60 * 1000).toISOString();

    case 'TODAY':
      // End of current day (11:59 PM) in America/Chicago timezone
      // Convert created date to Chicago timezone
      const chicagoDateStr = created.toLocaleString('en-US', { timeZone: 'America/Chicago' });
      const chicagoDate = new Date(chicagoDateStr);

      // Set to end of day in Chicago time
      chicagoDate.setHours(23, 59, 59, 999);

      // Convert back to UTC/ISO format
      return chicagoDate.toISOString();

    case '24H':
      // 24 hours from creation
      return new Date(created.getTime() + 24 * 60 * 60 * 1000).toISOString();

    case 'WEEK':
      // 7 days from creation
      return new Date(created.getTime() + 7 * 24 * 60 * 60 * 1000).toISOString();

    default:
      // Default to TODAY
      const defaultChicagoStr = new Date().toLocaleString('en-US', { timeZone: 'America/Chicago' });
      const defaultChicago = new Date(defaultChicagoStr);
      defaultChicago.setHours(23, 59, 59, 999);
      return defaultChicago.toISOString();
  }
}

// Calculate SLA status based on deadline
function calculateSlaStatus(slaDeadline, createdAt, sla) {
  const now = Date.now();
  const deadline = new Date(slaDeadline).getTime();
  const created = new Date(createdAt).getTime();

  // Calculate original SLA duration
  let slaDuration;
  switch (sla) {
    case 'NOW':
      slaDuration = 60 * 60 * 1000; // 1 hour
      break;
    case 'TODAY':
      slaDuration = deadline - created;
      break;
    case '24H':
      slaDuration = 24 * 60 * 60 * 1000;
      break;
    case 'WEEK':
      slaDuration = 7 * 24 * 60 * 60 * 1000;
      break;
    default:
      slaDuration = deadline - created;
  }

  const timeRemaining = deadline - now;
  const timeElapsed = now - deadline;

  // Escalated: past 2x original SLA duration
  if (timeElapsed > slaDuration) {
    return 'escalated';
  }

  // Overdue: past deadline
  if (now > deadline) {
    return 'overdue';
  }

  // Warning: less than 50% time remaining
  const totalTime = deadline - created;
  const percentRemaining = timeRemaining / totalTime;

  if (percentRemaining < 0.5) {
    return 'warning';
  }

  // On track: more than 50% time remaining
  return 'on_track';
}

// Format time left as human readable string
function formatTimeLeft(slaDeadline, slaStatus) {
  const now = Date.now();
  const deadline = new Date(slaDeadline).getTime();
  const diff = Math.abs(deadline - now);

  const minutes = Math.floor(diff / (60 * 1000));
  const hours = Math.floor(diff / (60 * 60 * 1000));
  const days = Math.floor(diff / (24 * 60 * 60 * 1000));

  let timeStr;
  if (days > 0) {
    timeStr = `${days}d`;
  } else if (hours > 0) {
    timeStr = `${hours}h`;
  } else {
    timeStr = `${minutes}m`;
  }

  if (slaStatus === 'overdue' || slaStatus === 'escalated') {
    return `OVERDUE ${timeStr}`;
  } else {
    return `${timeStr} left`;
  }
}

// Run SLA enforcement on all active tasks
async function runSlaEnforcement() {
  const now = new Date().toISOString();
  let changesCount = 0;

  const tasks = await db.getAllTasks({ status: 'pending,in_progress,assigned,blocked' });

  for (const task of tasks) {
    // Only enforce on active tasks
    if (!['pending', 'in_progress', 'assigned', 'blocked'].includes(task.status)) {
      continue;
    }

    // Skip if no SLA deadline
    if (!task.slaDeadline) {
      continue;
    }

    // Calculate current SLA status
    const newStatus = calculateSlaStatus(task.slaDeadline, task.createdAt, task.sla);
    const oldStatus = task.slaStatus;

    const updates = {
      slaStatus: newStatus,
      timeLeft: formatTimeLeft(task.slaDeadline, newStatus)
    };

    // Auto-block if overdue (but not already blocked)
    if (newStatus === 'overdue' && !task.slaOverdueAt) {
      updates.slaOverdueAt = now;

      // Auto-change to blocked if pending/in_progress/assigned
      if (['pending', 'in_progress', 'assigned'].includes(task.status)) {
        updates.status = 'blocked';
        updates.detail = task.detail ? `${task.detail}\n\n[SLA OVERDUE — auto-blocked]` : '[SLA OVERDUE — auto-blocked]';
        changesCount++;
      }
    }

    // Escalate if past 2x duration
    if (newStatus === 'escalated' && !task.slaEscalatedAt) {
      updates.slaEscalatedAt = now;

      // If Bot-owned, mark as botBlocked
      if (task.owner === 'Bot') {
        updates.botBlocked = true;
        updates.title = task.title.startsWith('⚠️') ? task.title : `⚠️ BOT BLOCKED: ${task.title}`;
        changesCount++;
      }
    }

    if (Object.keys(updates).length > 2) { // More than just slaStatus and timeLeft
      await db.updateTask(task.id, updates);
    }
  }

  return changesCount;
}

// Sort tasks by impact (revenue first, then severity, then needs_approval, then overdue, then updatedAt)
function sortByImpact(tasks) {
  return tasks.sort((a, b) => {
    // 0. Overdue/escalated first
    const aOverdue = (a.slaStatus === 'overdue' || a.slaStatus === 'escalated') ? 1 : 0;
    const bOverdue = (b.slaStatus === 'overdue' || b.slaStatus === 'escalated') ? 1 : 0;
    if (aOverdue !== bOverdue) return bOverdue - aOverdue;

    // 1. Revenue impact first (highest $ first)
    const aValue = parseEstimatedValue(a.estimatedValue);
    const bValue = parseEstimatedValue(b.estimatedValue);
    if (aValue !== bValue) return bValue - aValue;

    // 2. Severity HIGH blockers
    const severityOrder = { HIGH: 3, MEDIUM: 2, LOW: 1 };
    const aSeverity = severityOrder[a.severity] || 0;
    const bSeverity = severityOrder[b.severity] || 0;
    if (aSeverity !== bSeverity) return bSeverity - aSeverity;

    // 3. needs_approval
    const aApproval = a.status === 'needs_approval' ? 1 : 0;
    const bApproval = b.status === 'needs_approval' ? 1 : 0;
    if (aApproval !== bApproval) return bApproval - aApproval;

    // 4. Most recent first
    return new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime();
  });
}

// ========== AGENT-LOOP ENDPOINTS ==========

// POST /api/tasks — Create task (agent loop format)
app.post('/api/tasks', async (req, res) => {
  try {
    const {
      company,
      category,
      title,
      detail,
      priority,
      status
    } = req.body;

    if (!company || !category || !title) {
      return res.status(400).json({ error: 'Missing required fields: company, category, title' });
    }

    const now = new Date().toISOString();

    // Generate task ID in agent-loop format
    const taskId = `task-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

    // Map priority to SLA
    const priorityToSla = {
      high: 'NOW',
      medium: 'TODAY',
      low: '24H'
    };
    const taskSla = priorityToSla[priority] || 'TODAY';
    const slaDeadline = calculateSlaDeadline(taskSla, now);
    const slaStatus = calculateSlaStatus(slaDeadline, now, taskSla);
    const timeLeft = formatTimeLeft(slaDeadline, slaStatus);

    const task = {
      id: taskId,
      company,
      category,
      title,
      detail: detail || '',
      owner: 'Bot',
      trigger: 'Agent',
      status: status || 'queued',
      estimatedValue: null,
      tier: priority === 'high' ? 'HIGH' : priority === 'medium' ? 'MEDIUM' : 'LOW',
      impact: determineImpact(category, title, detail || ''),
      severity: null,
      sourceUrl: null,
      dueDate: null,
      assignee: null,
      agent: null,
      currentStep: null,
      nextAction: null,
      lastActionAt: now,
      nextActionAt: null,
      createdAt: now,
      updatedAt: now,
      startedAt: null,
      completedAt: null,
      sla: taskSla,
      slaDeadline,
      slaStatus,
      timeLeft,
      slaOverdueAt: null,
      slaEscalatedAt: null,
      botBlocked: false,
      rolledForward: false,
      originalPriority: null
    };

    const createdTask = await db.createTask(task);
    res.json(createdTask);
  } catch (err) {
    console.error('Task creation error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/tasks/claim — Agent claims next queued task
app.post('/api/tasks/claim', async (req, res) => {
  try {
    const { agentName, company } = req.body;

    if (!agentName) {
      return res.status(400).json({ error: 'Missing required field: agentName' });
    }

    // Check if agent already has a task in progress
    const agentInProgress = await db.findTaskByAgent(agentName);

    if (agentInProgress) {
      return res.status(409).json({
        error: 'Agent already has a task in progress',
        currentTask: agentInProgress
      });
    }

    // Find first queued task matching company (if specified)
    const queuedTask = await db.findQueuedTask(company);

    if (!queuedTask) {
      return res.json({ task: null, message: 'Queue empty' });
    }

    // Claim the task
    const now = new Date().toISOString();
    const updatedTask = await db.updateTask(queuedTask.id, {
      status: 'in_progress',
      agent: agentName,
      startedAt: now,
      lastActionAt: now
    });

    res.json({ task: updatedTask });
  } catch (err) {
    console.error('Task claim error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/agents — Register agent
app.post('/api/agents', async (req, res) => {
  try {
    const { name, company, type, description, capabilities } = req.body;

    if (!name) {
      return res.status(400).json({ error: 'Missing required field: name' });
    }

    const now = new Date().toISOString();

    // Find existing agent
    const existing = await db.getAgentByName(name);

    const agent = {
      id: existing ? existing.id : generateId(),
      name,
      company: company || null,
      type: type || 'background',
      description: description || '',
      capabilities: capabilities || [],
      currentTaskId: null,
      currentTask: null,
      error: null,
      lastAction: null,
      lastHeartbeat: now,
      status: 'idle',
      createdAt: existing ? existing.createdAt : now,
      updatedAt: now
    };

    const upsertedAgent = await db.upsertAgent(agent);
    res.json(upsertedAgent);
  } catch (err) {
    console.error('Agent registration error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/agents/:name/heartbeat — Agent heartbeat
app.post('/api/agents/:name/heartbeat', async (req, res) => {
  try {
    const { name } = req.params;
    const { currentTaskId, lastAction, error } = req.body;

    const agent = await db.getAgentByName(name);
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }

    const now = new Date().toISOString();

    const updates = {
      lastHeartbeat: now,
      lastAction: lastAction || agent.lastAction,
      error: error || null
    };

    if (currentTaskId) {
      updates.currentTaskId = currentTaskId;
      updates.status = 'working';

      // Find the task
      const task = await db.getTaskById(currentTaskId);
      if (task) {
        updates.currentTask = {
          id: task.id,
          title: task.title,
          company: task.company,
          status: task.status
        };
      }
    } else {
      updates.currentTaskId = null;
      updates.currentTask = null;
      updates.status = 'idle';
    }

    await db.updateAgent(name, updates);
    res.json({ ok: true });
  } catch (err) {
    console.error('Agent heartbeat error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/agents/health — Agent health summary
app.get('/api/agents/health', async (req, res) => {
  try {
    const now = Date.now();
    const agents = await db.getAllAgents();

    const healthSummary = agents.map(agent => {
      const lastHeartbeat = agent.lastHeartbeat ? new Date(agent.lastHeartbeat).getTime() : 0;
      const staleSec = Math.floor((now - lastHeartbeat) / 1000);
      const stale = staleSec > 120; // Stale if >120s since heartbeat

      return {
        ...agent,
        staleSec,
        stale
      };
    });

    res.json(healthSummary);
  } catch (err) {
    console.error('Agent health error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/tasks/:id/start — Start task execution
app.post('/api/tasks/:id/start', async (req, res) => {
  try {
    const { id } = req.params;
    const { agent, currentStep } = req.body;

    const task = await db.getTaskById(id);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const now = new Date().toISOString();
    const updatedTask = await db.updateTask(id, {
      status: 'in_progress',
      startedAt: now,
      agent: agent || task.agent,
      currentStep: currentStep || null,
      lastActionAt: now
    });

    res.json({ success: true, task: updatedTask });
  } catch (err) {
    console.error('Task start error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/tasks/:id/complete — Complete task
app.post('/api/tasks/:id/complete', async (req, res) => {
  try {
    const { id } = req.params;
    const { detail } = req.body;

    const task = await db.getTaskById(id);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const now = new Date().toISOString();
    const updates = {
      status: 'completed',
      completedAt: now,
      lastActionAt: now
    };

    if (detail) {
      updates.detail = task.detail ? `${task.detail}\n\n[COMPLETED] ${detail}` : detail;
    }

    const updatedTask = await db.updateTask(id, updates);
    res.json({ success: true, task: updatedTask });
  } catch (err) {
    console.error('Task complete error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/tasks/:id/fail — Fail task
app.post('/api/tasks/:id/fail', async (req, res) => {
  try {
    const { id } = req.params;
    const { error, blocked } = req.body;

    const task = await db.getTaskById(id);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const now = new Date().toISOString();
    const updates = {
      status: blocked ? 'blocked' : 'failed',
      error: error || 'Task failed',
      lastActionAt: now
    };

    if (error) {
      updates.detail = task.detail ? `${task.detail}\n\n[ERROR] ${error}` : `[ERROR] ${error}`;
    }

    const updatedTask = await db.updateTask(id, updates);
    res.json({ success: true, task: updatedTask });
  } catch (err) {
    console.error('Task fail error:', err);
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/tasks/:id/step — Update task step
app.patch('/api/tasks/:id/step', async (req, res) => {
  try {
    const { id } = req.params;
    const { currentStep, nextAction } = req.body;

    const task = await db.getTaskById(id);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const now = new Date().toISOString();
    const updates = { lastActionAt: now };

    if (currentStep !== undefined) {
      updates.currentStep = currentStep;
    }

    if (nextAction !== undefined) {
      updates.nextAction = nextAction;
    }

    const updatedTask = await db.updateTask(id, updates);
    res.json({ success: true, task: updatedTask });
  } catch (err) {
    console.error('Task step update error:', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/agents/:id — Delete agent
app.delete('/api/agents/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const deleted = await db.deleteAgent(id);
    if (!deleted) {
      return res.status(404).json({ error: 'Agent not found' });
    }

    res.json({ success: true, agent: deleted });
  } catch (err) {
    console.error('Agent delete error:', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/tasks/:id — Delete task
app.delete('/api/tasks/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const deleted = await db.deleteTask(id);
    if (!deleted) {
      return res.status(404).json({ error: 'Task not found' });
    }

    res.json({ success: true, task: deleted });
  } catch (err) {
    console.error('Task delete error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ========== WEBHOOK ENDPOINTS ==========

// POST /api/webhook/wa-lead — WortheyFlow sends new lead
app.post('/api/webhook/wa-lead', async (req, res) => {
  try {
    const { name, email, phone, source, stage, salesperson } = req.body;

    if (!name) {
      return res.status(400).json({ error: 'Missing required field: name' });
    }

    const now = new Date().toISOString();
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();

    // Dedup: check if task with same title exists in last 1 hour
    const taskTitle = `New lead: ${name}`;
    const existing = await db.findTaskByTitle(taskTitle, oneHourAgo);

    if (existing) {
      return res.json({ created: false, task: existing, message: 'Duplicate task detected' });
    }

    // Create task
    const taskSla = 'NOW';
    const slaDeadline = calculateSlaDeadline(taskSla, now);
    const slaStatus = calculateSlaStatus(slaDeadline, now, taskSla);
    const timeLeft = formatTimeLeft(slaDeadline, slaStatus);

    const task = {
      id: generateId(),
      company: 'WA',
      category: 'crm',
      title: taskTitle,
      detail: `Source: ${source || 'Unknown'}\nStage: ${stage || 'New'}\nEmail: ${email || 'N/A'}\nPhone: ${phone || 'N/A'}\nSalesperson: ${salesperson || 'Unassigned'}`,
      owner: 'Bot',
      trigger: 'Webhook',
      status: 'queued',
      estimatedValue: null,
      tier: 'HIGH',
      impact: 'LEADS',
      severity: null,
      sourceUrl: null,
      dueDate: null,
      assignee: salesperson || null,
      agent: null,
      lastActionAt: now,
      nextAction: 'Contact lead',
      nextActionAt: now,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      sla: taskSla,
      slaDeadline,
      slaStatus,
      timeLeft,
      slaOverdueAt: null,
      slaEscalatedAt: null,
      botBlocked: false,
      rolledForward: false,
      originalPriority: null
    };

    const createdTask = await db.createTask(task);
    res.json({ created: true, task: createdTask });
  } catch (err) {
    console.error('WA lead webhook error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/webhook/oa-submission — OA sends new submission
app.post('/api/webhook/oa-submission', async (req, res) => {
  try {
    const { name, county, email, property_address } = req.body;

    if (!name) {
      return res.status(400).json({ error: 'Missing required field: name' });
    }

    const now = new Date().toISOString();
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();

    // Dedup: check if task with same title exists in last 1 hour
    const taskTitle = `New submission: ${name}${county ? ` (${county})` : ''}`;
    const existing = await db.findTaskByTitle(taskTitle, oneHourAgo);

    if (existing) {
      return res.json({ created: false, task: existing, message: 'Duplicate task detected' });
    }

    // Create task
    const taskSla = 'NOW';
    const slaDeadline = calculateSlaDeadline(taskSla, now);
    const slaStatus = calculateSlaStatus(slaDeadline, now, taskSla);
    const timeLeft = formatTimeLeft(slaDeadline, slaStatus);

    const task = {
      id: generateId(),
      company: 'OA',
      category: 'analysis',
      title: taskTitle,
      detail: `County: ${county || 'N/A'}\nEmail: ${email || 'N/A'}\nProperty: ${property_address || 'N/A'}`,
      owner: 'Bot',
      trigger: 'Webhook',
      status: 'queued',
      estimatedValue: null,
      tier: 'HIGH',
      impact: 'LEADS',
      severity: null,
      sourceUrl: null,
      dueDate: null,
      assignee: null,
      agent: null,
      lastActionAt: now,
      nextAction: 'Analyze submission',
      nextActionAt: now,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      sla: taskSla,
      slaDeadline,
      slaStatus,
      timeLeft,
      slaOverdueAt: null,
      slaEscalatedAt: null,
      botBlocked: false,
      rolledForward: false,
      originalPriority: null
    };

    const createdTask = await db.createTask(task);
    res.json({ created: true, task: createdTask });
  } catch (err) {
    console.error('OA submission webhook error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ========== TASK ENDPOINTS ==========

// POST /api/task - Create a task
app.post('/api/task', async (req, res) => {
  try {
    const {
      company,
      category,
      title,
      detail,
      owner,
      trigger,
      status,
      estimatedValue,
      tier,
      impact,
      severity,
      sourceUrl,
      dueDate,
      assignee,
      nextAction,
      nextActionAt,
      sla
    } = req.body;

    if (!company || !category || !title) {
      return res.status(400).json({ error: 'Missing required fields: company, category, title' });
    }

    // Owner is required
    if (!owner) {
      return res.status(400).json({ error: 'Missing required field: owner' });
    }

    const now = new Date().toISOString();

    // Default SLA to TODAY if not provided
    const taskSla = sla || 'TODAY';
    const slaDeadline = calculateSlaDeadline(taskSla, now);
    const slaStatus = calculateSlaStatus(slaDeadline, now, taskSla);
    const timeLeft = formatTimeLeft(slaDeadline, slaStatus);

    const task = {
      id: generateId(),
      company,
      category,
      title,
      detail: detail || '',
      owner,
      trigger: trigger || 'Manual',
      status: status || 'pending',
      estimatedValue: estimatedValue || null,
      tier: tier || (estimatedValue ? calculateTier(estimatedValue) : 'LOW'),
      impact: impact || determineImpact(category, title, detail || ''),
      severity: severity || (status === 'blocked' ? determineSeverity(impact || 'OPERATIONS', title, detail || '') : null),
      sourceUrl: sourceUrl || null,
      dueDate: dueDate || null,
      assignee: assignee || null,
      lastActionAt: now,
      nextAction: nextAction || null,
      nextActionAt: nextActionAt || null,
      createdAt: now,
      updatedAt: now,
      completedAt: status === 'completed' ? now : null,
      // SLA fields
      sla: taskSla,
      slaDeadline,
      slaStatus,
      timeLeft,
      slaOverdueAt: null,
      slaEscalatedAt: null,
      botBlocked: false,
      rolledForward: false,
      originalPriority: null
    };

    const createdTask = await db.createTask(task);
    res.json({ success: true, task: createdTask });
  } catch (err) {
    console.error('Task creation error:', err);
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/task/:id - Update a task
app.patch('/api/task/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    const task = await db.getTaskById(id);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const now = new Date().toISOString();

    // Track if status changed
    const statusChanged = updates.status && updates.status !== task.status;
    const slaChanged = updates.sla && updates.sla !== task.sla;

    // Auto-set lastActionAt when status changes
    if (statusChanged) {
      updates.lastActionAt = now;
    }

    // Set completedAt if status changed to completed
    if (updates.status === 'completed' && !task.completedAt) {
      updates.completedAt = now;
    }

    // Recalculate tier if estimatedValue changed
    if (updates.estimatedValue) {
      updates.tier = calculateTier(updates.estimatedValue);
    }

    // Recalculate SLA deadline if sla changed
    if (slaChanged) {
      updates.slaDeadline = calculateSlaDeadline(updates.sla, task.createdAt);
      updates.slaStatus = calculateSlaStatus(updates.slaDeadline, task.createdAt, updates.sla);
      updates.timeLeft = formatTimeLeft(updates.slaDeadline, updates.slaStatus);
    }

    const updatedTask = await db.updateTask(id, updates);
    res.json({ success: true, task: updatedTask });
  } catch (err) {
    console.error('Task update error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/tasks - List tasks with filters
app.get('/api/tasks', async (req, res) => {
  try {
    const filters = {};
    if (req.query.company) filters.company = req.query.company;
    if (req.query.category) filters.category = req.query.category;
    if (req.query.status) filters.status = req.query.status;
    if (req.query.owner) filters.owner = req.query.owner;

    const tasks = await db.getAllTasks(filters);
    res.json(tasks);
  } catch (err) {
    console.error('Task list error:', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/task/:id - Soft delete (archive)
app.delete('/api/task/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const task = await db.getTaskById(id);

    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const archivedTask = await db.archiveTask(id);
    res.json({ success: true, task: archivedTask });
  } catch (err) {
    console.error('Task delete error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/daily-reset - Daily reset endpoint
app.post('/api/daily-reset', async (req, res) => {
  try {
    const now = new Date();

    // Calculate tomorrow EOD in Chicago timezone
    const chicagoNow = new Date(now.toLocaleString('en-US', { timeZone: 'America/Chicago' }));
    chicagoNow.setDate(chicagoNow.getDate() + 1);
    chicagoNow.setHours(23, 59, 59, 999);
    const tomorrowEod = chicagoNow.toISOString();

    let rolledCount = 0;

    // Get tasks with TODAY or NOW SLA that are incomplete
    const allTasks = await db.getAllTasks();

    for (const task of allTasks) {
      // Only roll forward incomplete tasks with TODAY or NOW SLA
      if (['pending', 'in_progress', 'assigned'].includes(task.status) &&
          (task.sla === 'TODAY' || task.sla === 'NOW')) {

        const updates = {
          rolledForward: true,
          sla: 'TODAY',
          slaDeadline: tomorrowEod,
          slaStatus: 'on_track',
          timeLeft: formatTimeLeft(tomorrowEod, 'on_track'),
          slaOverdueAt: null,
          slaEscalatedAt: null
        };

        // Boost priority
        if (!task.originalPriority) {
          updates.originalPriority = task.tier;
        }

        if (task.tier === 'LOW') {
          updates.tier = 'MEDIUM';
        } else if (task.tier === 'MEDIUM') {
          updates.tier = 'HIGH';
        }

        // Add note
        const rollNote = `[Rolled forward from ${now.toLocaleDateString('en-US', { timeZone: 'America/Chicago' })}]`;
        updates.detail = task.detail ? `${task.detail}\n${rollNote}` : rollNote;

        await db.updateTask(task.id, updates);
        rolledCount++;
      }
    }

    res.json({ success: true, rolledCount, message: `${rolledCount} tasks rolled forward` });
  } catch (err) {
    console.error('Daily reset error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ========== CREATOR PIPELINE ENDPOINTS ==========

// POST /api/creator - Create a creator
app.post('/api/creator', async (req, res) => {
  try {
    const {
      name,
      platform,
      handle,
      email,
      phone,
      audienceSize,
      niche,
      state,
      stage,
      lastContactDate,
      lastContactChannel,
      nextAction,
      nextActionDate,
      outreachDate,
      notes
    } = req.body;

    if (!name || !platform) {
      return res.status(400).json({ error: 'Missing required fields: name, platform' });
    }

    const now = new Date().toISOString();

    const creator = {
      id: generateId(),
      name,
      company: 'OA',
      platform,
      handle: handle || null,
      email: email || null,
      phone: phone || null,
      audienceSize: audienceSize || 0,
      niche: niche || '',
      state: state || null,
      stage: stage || 'outreach_sent',
      lastContactDate: lastContactDate || now,
      lastContactChannel: lastContactChannel || platform,
      nextAction: nextAction || 'Wait for reply',
      nextActionDate: nextActionDate || null,
      outreachDate: outreachDate || now,
      repliedAt: null,
      notes: notes || '',
      replyHistory: [],
      taskCreated: false,
      onboardingTaskCreated: false,
      createdAt: now,
      updatedAt: now
    };

    creators.unshift(creator);
    await persistData();

    res.json({ success: true, creator });
  } catch (err) {
    console.error('Creator creation error:', err);
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/creator/:id - Update a creator
app.patch('/api/creator/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    const creator = creators.find(c => c.id === id);
    if (!creator) {
      return res.status(404).json({ error: 'Creator not found' });
    }

    const now = new Date().toISOString();

    // Track if stage changed to 'replied'
    const stageChangedToReplied = updates.stage === 'replied' && creator.stage !== 'replied';

    // Apply updates
    Object.keys(updates).forEach(key => {
      if (key !== 'id' && key !== 'createdAt' && key !== 'company') {
        creator[key] = updates[key];
      }
    });

    creator.updatedAt = now;

    // Set repliedAt timestamp if stage changed to 'replied'
    if (stageChangedToReplied && !creator.repliedAt) {
      creator.repliedAt = now;

      // Add to reply history
      creator.replyHistory.push({
        date: now,
        channel: creator.lastContactChannel || creator.platform,
        preview: updates.notes || 'Reply received'
      });
    }

    await persistData();
    res.json({ success: true, creator });
  } catch (err) {
    console.error('Creator update error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/creators - List all creators with optional stage filter
app.get('/api/creators', (req, res) => {
  try {
    let filtered = [...creators];

    // Apply stage filter
    if (req.query.stage) {
      filtered = filtered.filter(c => c.stage === req.query.stage);
    }

    res.json(filtered);
  } catch (err) {
    console.error('Creator list error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/creators/import - Bulk import creators
app.post('/api/creators/import', async (req, res) => {
  try {
    const { creators: importedCreators } = req.body;

    if (!Array.isArray(importedCreators)) {
      return res.status(400).json({ error: 'creators must be an array' });
    }

    const now = new Date().toISOString();
    let importedCount = 0;

    importedCreators.forEach(c => {
      if (!c.name || !c.platform) {
        return; // Skip invalid entries
      }

      const creator = {
        id: generateId(),
        name: c.name,
        company: 'OA',
        platform: c.platform,
        handle: c.handle || null,
        email: c.email || null,
        phone: c.phone || null,
        audienceSize: c.audienceSize || 0,
        niche: c.niche || '',
        state: c.state || null,
        stage: c.stage || 'outreach_sent',
        lastContactDate: c.lastContactDate || now,
        lastContactChannel: c.lastContactChannel || c.platform,
        nextAction: c.nextAction || 'Wait for reply',
        nextActionDate: c.nextActionDate || null,
        outreachDate: c.outreachDate || now,
        repliedAt: c.repliedAt || null,
        notes: c.notes || '',
        replyHistory: c.replyHistory || [],
        taskCreated: false,
        onboardingTaskCreated: false,
        createdAt: now,
        updatedAt: now
      };

      creators.unshift(creator);
      importedCount++;
    });

    await persistData();
    res.json({ success: true, importedCount });
  } catch (err) {
    console.error('Creator import error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/creator-pipeline - Pipeline summary for dashboard
app.get('/api/creator-pipeline', async (req, res) => {
  try {
    // Run follow-up automation
    await runCreatorFollowUp();

    // Group by stage
    const stages = {
      outreach_sent: creators.filter(c => c.stage === 'outreach_sent'),
      replied: creators.filter(c => c.stage === 'replied'),
      interested: creators.filter(c => c.stage === 'interested'),
      negotiation: creators.filter(c => c.stage === 'negotiation'),
      committed: creators.filter(c => c.stage === 'committed'),
      active: creators.filter(c => c.stage === 'active')
    };

    // Calculate stats
    const totalCreators = creators.length;
    const totalReplies = creators.filter(c => c.repliedAt).length;
    const replyRate = totalCreators > 0 ? ((totalReplies / totalCreators) * 100).toFixed(1) : '0.0';

    const totalActive = stages.active.length;
    const conversionRate = totalCreators > 0 ? ((totalActive / totalCreators) * 100).toFixed(1) : '0.0';

    const totalAudience = creators.reduce((sum, c) => sum + (c.audienceSize || 0), 0);

    // Find creators needing attention (overdue follow-ups)
    const now = new Date();
    const needsAttention = creators.filter(c => {
      if (!c.nextActionDate) return false;
      const actionDate = new Date(c.nextActionDate);
      return actionDate < now && c.stage !== 'active' && c.stage !== 'cold';
    });

    const stats = {
      total: totalCreators,
      replyRate: `${replyRate}%`,
      conversionRate: `${conversionRate}%`,
      totalAudience
    };

    res.json({ stages, stats, needsAttention });
  } catch (err) {
    console.error('Creator pipeline error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ========== DASHBOARD ENDPOINT ==========

app.get('/api/dashboard', async (req, res) => {
  try {
    // Run SLA enforcement on every dashboard request
    await runSlaEnforcement();

    const [oaData, waData, mpData, smsData, commsData, tasks] = await Promise.all([
      fetchOAData(),
      fetchWAData(),
      fetchMPData(),
      fetch('https://disciplined-alignment-production.up.railway.app/api/sms-status')
        .then(r => r.json()).catch(() => ({ status: 'UNKNOWN', successRate: 'N/A' })),
      fetch('https://disciplined-alignment-production.up.railway.app/api/comms-status')
        .then(r => r.json()).catch(() => ({ rates: {}, stats: {}, noReply24h: 0 })),
      db.getAllTasks()
    ]);

    const now = Date.now();
    const last24h = now - (24 * 60 * 60 * 1000);

    // Leads today: WA leads created in last 24h
    const waLeadsToday = waData.leads?.filter(l =>
      l.created_at && new Date(l.created_at).getTime() > last24h
    ).length || 0;

    const leadsToday = waLeadsToday;

    // Decisions pending
    const decisionsPending = tasks.filter(t => t.status === 'needs_approval').length;

    // Pipeline value
    const pipelineValue = tasks
      .filter(t => t.status === 'needs_approval')
      .reduce((sum, t) => sum + parseEstimatedValue(t.estimatedValue), 0);

    // Tasks by company (non-completed, non-archived)
    const activeTasks = tasks.filter(t =>
      t.status !== 'completed' && t.status !== 'archived'
    );
    const tasksByCompany = {
      WA: activeTasks.filter(t => t.company === 'WA').length,
      OA: activeTasks.filter(t => t.company === 'OA').length,
      MP: activeTasks.filter(t => t.company === 'MP').length,
      PBC: activeTasks.filter(t => t.company === 'PBC').length
    };

    // Blocked count
    const blockedCount = tasks.filter(t => t.status === 'blocked').length;

    // Overdue count
    const overdueCount = tasks.filter(t =>
      t.slaStatus === 'overdue' || t.slaStatus === 'escalated'
    ).length;

    // Escalated count
    const escalatedCount = tasks.filter(t => t.slaStatus === 'escalated').length;

    // === TYLER'S VIEW ===
    const tylerDecisions = sortByImpact(
      tasks.filter(t => t.status === 'needs_approval')
    );

    const tylerTasks = sortByImpact(
      tasks.filter(t =>
        t.owner === 'Tyler' &&
        (t.status === 'pending' || t.status === 'in_progress' || t.status === 'assigned')
      )
    );

    const tylerOverdue = sortByImpact(
      tasks.filter(t =>
        t.owner === 'Tyler' &&
        (t.slaStatus === 'overdue' || t.slaStatus === 'escalated')
      )
    );

    const tylerBlocked = sortByImpact(
      tasks.filter(t => t.owner === 'Tyler' && t.status === 'blocked')
    );

    // === BOT VIEW ===
    const botActive = sortByImpact(
      tasks.filter(t => t.owner === 'Bot' && t.status === 'in_progress')
    );

    const botWaiting = sortByImpact(
      tasks.filter(t => t.owner === 'Bot' && (t.status === 'pending' || t.status === 'assigned'))
    );

    const botBlocked = sortByImpact(
      tasks.filter(t => t.owner === 'Bot' && t.botBlocked === true)
    );

    const botRecentCompleted = tasks
      .filter(t => t.owner === 'Bot' && t.status === 'completed' && t.completedAt && new Date(t.completedAt).getTime() > last24h)
      .sort((a, b) => new Date(b.completedAt).getTime() - new Date(a.completedAt).getTime())
      .slice(0, 10);

    // === TEAM VIEW ===
    const humanNames = ['Anibal', 'Ricardo', 'Valerie', 'Richard', 'Paul'];
    const teamTasks = sortByImpact(
      tasks.filter(t =>
        humanNames.includes(t.owner || t.assignee) &&
        t.status !== 'completed' &&
        t.status !== 'archived'
      )
    );

    // === BLOCKERS (all, sorted by REVENUE/LEADS first) ===
    const allBlockers = tasks.filter(t => t.status === 'blocked');
    const blockers = allBlockers.sort((a, b) => {
      // REVENUE impact first
      if (a.impact === 'REVENUE' && b.impact !== 'REVENUE') return -1;
      if (b.impact === 'REVENUE' && a.impact !== 'REVENUE') return 1;

      // Then LEADS impact
      if (a.impact === 'LEADS' && b.impact !== 'LEADS') return -1;
      if (b.impact === 'LEADS' && a.impact !== 'LEADS') return 1;

      // Then by severity
      const severityOrder = { HIGH: 3, MEDIUM: 2, LOW: 1 };
      return (severityOrder[b.severity] || 0) - (severityOrder[a.severity] || 0);
    });

    // === COMPLETED TODAY ===
    const completed = tasks
      .filter(t => t.status === 'completed' && t.completedAt && new Date(t.completedAt).getTime() > last24h)
      .sort((a, b) => new Date(b.completedAt).getTime() - new Date(a.completedAt).getTime())
      .slice(0, 20);

    // === COMPANY VIEWS ===
    const byCompany = {
      WA: {
        revenue: sortByImpact(tasks.filter(t => t.company === 'WA' && t.category === 'revenue' && t.status !== 'completed' && t.status !== 'archived')),
        operations: sortByImpact(tasks.filter(t => t.company === 'WA' && t.category === 'operations' && t.status !== 'completed' && t.status !== 'archived')),
        marketing: sortByImpact(tasks.filter(t => t.company === 'WA' && t.category === 'marketing' && t.status !== 'completed' && t.status !== 'archived')),
        product: sortByImpact(tasks.filter(t => t.company === 'WA' && t.category === 'product' && t.status !== 'completed' && t.status !== 'archived'))
      },
      OA: {
        revenue: sortByImpact(tasks.filter(t => t.company === 'OA' && t.category === 'revenue' && t.status !== 'completed' && t.status !== 'archived')),
        operations: sortByImpact(tasks.filter(t => t.company === 'OA' && t.category === 'operations' && t.status !== 'completed' && t.status !== 'archived')),
        marketing: sortByImpact(tasks.filter(t => t.company === 'OA' && t.category === 'marketing' && t.status !== 'completed' && t.status !== 'archived')),
        product: sortByImpact(tasks.filter(t => t.company === 'OA' && t.category === 'product' && t.status !== 'completed' && t.status !== 'archived'))
      },
      MP: {
        revenue: sortByImpact(tasks.filter(t => t.company === 'MP' && t.category === 'revenue' && t.status !== 'completed' && t.status !== 'archived')),
        operations: sortByImpact(tasks.filter(t => t.company === 'MP' && t.category === 'operations' && t.status !== 'completed' && t.status !== 'archived')),
        marketing: sortByImpact(tasks.filter(t => t.company === 'MP' && t.category === 'marketing' && t.status !== 'completed' && t.status !== 'archived')),
        product: sortByImpact(tasks.filter(t => t.company === 'MP' && t.category === 'product' && t.status !== 'completed' && t.status !== 'archived'))
      },
      PBC: {
        revenue: sortByImpact(tasks.filter(t => t.company === 'PBC' && t.category === 'revenue' && t.status !== 'completed' && t.status !== 'archived')),
        operations: sortByImpact(tasks.filter(t => t.company === 'PBC' && t.category === 'operations' && t.status !== 'completed' && t.status !== 'archived')),
        marketing: sortByImpact(tasks.filter(t => t.company === 'PBC' && t.category === 'marketing' && t.status !== 'completed' && t.status !== 'archived')),
        product: sortByImpact(tasks.filter(t => t.company === 'PBC' && t.category === 'product' && t.status !== 'completed' && t.status !== 'archived'))
      }
    };

    // Creator pipeline summary
    const totalCreators = creators.length;
    const creatorsReplied = creators.filter(c => c.repliedAt).length;
    const creatorsNeedAttention = creators.filter(c => {
      if (!c.nextActionDate) return false;
      const actionDate = new Date(c.nextActionDate);
      return actionDate < now && c.stage !== 'active' && c.stage !== 'cold';
    }).length;

    // Recent replies (last 5)
    const recentReplies = creators
      .filter(c => c.repliedAt)
      .sort((a, b) => new Date(b.repliedAt).getTime() - new Date(a.repliedAt).getTime())
      .slice(0, 5)
      .map(c => ({
        name: c.name,
        platform: c.platform,
        repliedAt: c.repliedAt
      }));

    res.json({
      timestamp: new Date().toISOString(),
      stats: {
        leadsToday,
        decisionsPending,
        pipelineValue,
        tasksByCompany,
        blockedCount,
        overdueCount,
        escalatedCount
      },
      creatorPipeline: {
        total: totalCreators,
        replied: creatorsReplied,
        needsAttention: creatorsNeedAttention,
        recentReplies
      },
      sms: {
        status: smsData.status || 'UNKNOWN',
        successRate: smsData.successRate || 'N/A',
        isBlocker: smsData.isBlocker || false,
        fallbacksToEmail: smsData.fallbacksToEmail || 0,
        failures: smsData.failures || 0
      },
      communications: {
        deliveryRate: commsData.rates?.deliveryRate || 'N/A',
        openRate: commsData.rates?.openRate || 'N/A',
        responseRate: commsData.rates?.responseRate || 'N/A',
        bounceRate: commsData.rates?.bounceRate || 'N/A',
        noReply24h: commsData.noReply24h || 0,
        totalSent: commsData.stats?.sent || 0,
        totalDelivered: commsData.stats?.delivered || 0,
        totalOpened: commsData.stats?.opened || 0,
        totalReplied: commsData.stats?.replied || 0
      },
      tylerTasks: {
        decisions: tylerDecisions,
        myTasks: tylerTasks,
        overdue: tylerOverdue,
        blocked: tylerBlocked
      },
      botTasks: {
        active: botActive,
        waiting: botWaiting,
        blocked: botBlocked,
        recentCompleted: botRecentCompleted
      },
      teamTasks,
      blockers,
      completed,
      byCompany,
      sources: {
        oa: oaData.error ? { error: oaData.error } : {
          clientCount: oaData.clients.length,
          submissionCount: oaData.submissions.length
        },
        wa: waData.error ? { error: waData.error } : {
          leadCount: waData.leadCount
        },
        mp: mpData.error ? { error: mpData.error } : {
          userCount: mpData.userCount,
          tripCount: mpData.tripCount
        }
      }
    });
  } catch (err) {
    console.error('Dashboard error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ========== LEGACY ENDPOINTS (for backward compatibility) ==========

// POST /api/activity - Create activity (maps to task)
app.post('/api/activity', async (req, res) => {
  try {
    const { company, action, detail, type, agent, sourceUrl } = req.body;

    if (!company || !action) {
      return res.status(400).json({ error: 'Missing required fields: company, action' });
    }

    // Add to legacy activity log
    const entry = {
      id: generateId(),
      company,
      action,
      detail: detail || '',
      type: type || 'activity',
      agent: agent || 'System',
      sourceUrl: sourceUrl || null,
      timestamp: new Date().toISOString()
    };

    activityLog.unshift(entry);
    if (activityLog.length > 500) {
      activityLog = activityLog.slice(0, 500);
    }

    // Also create a task if type is meaningful
    if (type === 'activity' || type === 'completed') {
      const now = entry.timestamp;
      const taskSla = 'TODAY';
      const slaDeadline = calculateSlaDeadline(taskSla, now);
      const slaStatus = calculateSlaStatus(slaDeadline, now, taskSla);

      const task = {
        id: generateId(),
        company,
        category: 'operations',
        title: action,
        detail: detail || '',
        owner: agent || 'Bot',
        trigger: 'System',
        status: type === 'completed' ? 'completed' : 'in_progress',
        estimatedValue: null,
        tier: 'LOW',
        impact: 'OPERATIONS',
        severity: null,
        sourceUrl: sourceUrl || null,
        dueDate: null,
        assignee: null,
        lastActionAt: entry.timestamp,
        nextAction: null,
        nextActionAt: null,
        createdAt: entry.timestamp,
        updatedAt: entry.timestamp,
        completedAt: type === 'completed' ? entry.timestamp : null,
        sla: taskSla,
        slaDeadline,
        slaStatus,
        timeLeft: formatTimeLeft(slaDeadline, slaStatus),
        slaOverdueAt: null,
        slaEscalatedAt: null,
        botBlocked: false,
        rolledForward: false,
        originalPriority: null
      };

      await db.createTask(task);
    }

    await persistData();
    res.json({ success: true, entry });
  } catch (err) {
    console.error('Activity log error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/approval - Create approval (maps to needs_approval task)
app.post('/api/approval', async (req, res) => {
  try {
    const {
      company,
      title,
      detail,
      estimatedValue,
      tier,
      sourceUrl
    } = req.body;

    if (!company || !title) {
      return res.status(400).json({ error: 'Missing required fields: company, title' });
    }

    const now = new Date().toISOString();
    const taskSla = 'TODAY';
    const slaDeadline = calculateSlaDeadline(taskSla, now);
    const slaStatus = calculateSlaStatus(slaDeadline, now, taskSla);

    const task = {
      id: generateId(),
      company,
      category: 'revenue',
      title,
      detail: detail || '',
      owner: 'Bot',
      trigger: 'System',
      status: 'needs_approval',
      estimatedValue: estimatedValue || null,
      tier: tier || (estimatedValue ? calculateTier(estimatedValue) : 'MEDIUM'),
      impact: 'REVENUE',
      severity: null,
      sourceUrl: sourceUrl || null,
      dueDate: null,
      assignee: null,
      lastActionAt: now,
      nextAction: null,
      nextActionAt: null,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      sla: taskSla,
      slaDeadline,
      slaStatus,
      timeLeft: formatTimeLeft(slaDeadline, slaStatus),
      slaOverdueAt: null,
      slaEscalatedAt: null,
      botBlocked: false,
      rolledForward: false,
      originalPriority: null
    };

    const created = await db.createTask(task);

    await persistData();
    res.json({ success: true, approval: created });
  } catch (err) {
    console.error('Approval creation error:', err);
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/approval/:id - Update approval (maps to task update)
app.patch('/api/approval/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { action, reason } = req.body;

    const task = await db.getTaskById(id);
    if (!task) {
      return res.status(404).json({ error: 'Approval not found' });
    }

    const now = new Date().toISOString();
    const updates = { updatedAt: now };

    if (action === 'approve') {
      updates.status = 'completed';
      updates.completedAt = now;
      updates.lastActionAt = now;
    } else if (action === 'reject') {
      updates.status = 'archived';
      updates.detail = `${task.detail}\n[REJECTED: ${reason || 'No reason provided'}]`.trim();
      updates.lastActionAt = now;
    }

    const updated = await db.updateTask(id, updates);

    await persistData();
    res.json({ success: true, approval: updated });
  } catch (err) {
    console.error('Approval update error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/blocker - Create blocker (maps to blocked task)
app.post('/api/blocker', async (req, res) => {
  try {
    const { company, title, detail, severity, impact, sourceUrl } = req.body;

    if (!company || !title) {
      return res.status(400).json({ error: 'Missing required fields: company, title' });
    }

    const now = new Date().toISOString();
    const taskSla = 'NOW';
    const slaDeadline = calculateSlaDeadline(taskSla, now);
    const slaStatus = calculateSlaStatus(slaDeadline, now, taskSla);

    const calculatedImpact = impact || determineImpact('operations', title, detail || '');
    const calculatedSeverity = severity || determineSeverity(calculatedImpact, title, detail || '');

    const task = {
      id: generateId(),
      company,
      category: 'operations',
      title,
      detail: detail || '',
      owner: 'Bot',
      trigger: 'System',
      status: 'blocked',
      estimatedValue: null,
      tier: 'LOW',
      impact: calculatedImpact,
      severity: calculatedSeverity,
      sourceUrl: sourceUrl || null,
      dueDate: null,
      assignee: null,
      lastActionAt: now,
      nextAction: null,
      nextActionAt: null,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      sla: taskSla,
      slaDeadline,
      slaStatus,
      timeLeft: formatTimeLeft(slaDeadline, slaStatus),
      slaOverdueAt: null,
      slaEscalatedAt: null,
      botBlocked: false,
      rolledForward: false,
      originalPriority: null
    };

    const created = await db.createTask(task);

    await persistData();
    res.json({ success: true, blocker: created });
  } catch (err) {
    console.error('Blocker creation error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/reset - Clear all data
app.post('/api/reset', async (req, res) => {
  try {
    const { target } = req.body || {};
    if (target === 'all') {
      // Note: tasks/agents in Supabase — reset would require DELETE FROM mc_tasks/mc_agents
      // For safety, only clear local data (creators/activity)
      activityLog.length = 0;
      creators.length = 0;
      await persistData();
      return res.json({ success: true, message: 'Local data cleared (tasks/agents in Supabase — use SQL to reset)' });
    }
    if (target === 'creators') {
      creators.length = 0;
      await persistData();
      return res.json({ success: true, message: 'Creators cleared', count: 0 });
    }
    res.status(400).json({ error: 'Send { target: "all" }' });
  } catch (err) {
    console.error('Reset error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ========== HEALTH & STATIC ==========

// GET /api/agents — Agent status for dashboard visibility
app.get('/api/agents', async (req, res) => {
  try {
    const agents = await db.getAllAgents();
    res.json(agents);
  } catch (err) {
    console.error('Agent list error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/agent-heartbeat — Agents report their status (legacy endpoint)
app.post('/api/agent-heartbeat', async (req, res) => {
  try {
    const { name, status, task, type, lastSeen } = req.body;
    if (!name) return res.status(400).json({ error: 'name required' });
    
    const existing = await db.getAgentByName(name);
    const now = new Date().toISOString();
    
    const agent = {
      id: existing ? existing.id : `agent-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      name,
      status: status || 'active',
      task: task || '',
      type: type || 'background',
      lastHeartbeat: lastSeen || now,
      updatedAt: now,
      company: existing ? existing.company : null,
      description: existing ? existing.description : '',
      capabilities: existing ? existing.capabilities : [],
      createdAt: existing ? existing.createdAt : now
    };
    
    const result = await db.upsertAgent(agent);
    res.json({ ok: true, agent: result });
  } catch (err) {
    console.error('Agent heartbeat error:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/health', async (req, res) => {
  try {
    const now = Date.now();
    const uptimeSec = Math.floor((now - startTime) / 1000);
    const health = await db.healthCheck();
    
    res.json({
      ok: true,
      uptimeSec,
      storage: 'supabase',
      agents: health.agents,
      tasks: health.tasks
    });
  } catch (err) {
    console.error('Health check error:', err);
    res.json({
      ok: false,
      error: err.message,
      uptimeSec: Math.floor((Date.now() - startTime) / 1000)
    });
  }
});

app.get('/health', async (req, res) => {
  try {
    const now = Date.now();
    const uptimeSec = Math.floor((now - startTime) / 1000);
    const health = await db.healthCheck();
    
    res.json({
      status: 'ok',
      timestamp: new Date().toISOString(),
      uptimeSec,
      storage: 'supabase',
      taskCount: health.tasks.total,
      agentCount: health.agents.total
    });
  } catch (err) {
    res.json({ status: 'error', error: err.message });
  }
});

// ── Creator Reply Tracking ──
let creatorReplies = [];

// POST /api/creator-reply — log a detected creator reply
app.post('/api/creator-reply', (req, res) => {
  const { name, email, instagram, platform, subject, preview, detectedAt } = req.body;
  if (!name) return res.status(400).json({ error: 'name required' });
  
  const reply = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    name,
    email: email || null,
    instagram: instagram || null,
    platform: platform || 'email', // email, instagram, sms
    subject: subject || null,
    preview: preview || null,
    detectedAt: detectedAt || new Date().toISOString(),
    respondedAt: null,
    status: 'new' // new, responded, expired
  };
  
  creatorReplies.unshift(reply);
  
  // Auto-expire check: mark as expired if not responded to within 1 hour
  setTimeout(() => {
    const r = creatorReplies.find(cr => cr.id === reply.id);
    if (r && r.status === 'new') {
      r.status = 'expired';
      console.log(`🔴 Creator reply EXPIRED (1h SLA): ${r.name}`);
    }
  }, 60 * 60 * 1000);
  
  console.log(`📩 Creator reply logged: ${name} via ${platform}`);
  res.json({ success: true, reply });
});

// PATCH /api/creator-reply/:id — mark as responded
app.patch('/api/creator-reply/:id', (req, res) => {
  const reply = creatorReplies.find(r => r.id === req.params.id);
  if (!reply) return res.status(404).json({ error: 'not found' });
  reply.status = 'responded';
  reply.respondedAt = new Date().toISOString();
  res.json({ success: true, reply });
});

// GET /api/creator-replies — list replies
app.get('/api/creator-replies', (req, res) => {
  const today = new Date().toISOString().slice(0, 10);
  const todayReplies = creatorReplies.filter(r => r.detectedAt.slice(0, 10) === today);
  const unresponded = creatorReplies.filter(r => r.status === 'new');
  const expired = creatorReplies.filter(r => r.status === 'expired');
  
  res.json({
    today: todayReplies,
    unresponded,
    expired,
    total: creatorReplies.length
  });
});

// Serve index.html
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Start server
initDataStore().then(async () => {
  // Verify Supabase connection
  try {
    const health = await db.healthCheck();
    console.log(`📦 Supabase MC: ${health.agents.total} agents, ${health.tasks.total} tasks`);
  } catch (err) {
    console.error('⚠️ Supabase connection failed:', err.message);
    console.error('Falling back may not work — check SUPABASE_URL and key');
  }
  
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Mission Control V3 (Supabase-backed) running on port ${PORT}`);
  });
});
