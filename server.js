const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs').promises;
const path = require('path');

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

// Data directory
const DATA_DIR = path.join(__dirname, 'data');
const TASKS_FILE = path.join(DATA_DIR, 'tasks.json');
const ACTIVITY_FILE = path.join(DATA_DIR, 'activity.json');
const CREATORS_FILE = path.join(DATA_DIR, 'creators.json');
const AGENTS_FILE = path.join(DATA_DIR, 'agents.json');

// In-memory storage
let tasks = [];
let activityLog = [];
let creators = [];
let agentRegistry = [
  { name: 'OA Analysis Agent', status: 'active', task: 'Incoming lead analysis + 5-comp rule', type: 'background', lastSeen: new Date().toISOString() },
  { name: 'OA Ops Agent', status: 'active', task: 'Lead contact within 60s', type: 'background', lastSeen: new Date().toISOString() },
  { name: 'WA CRM Agent', status: 'active', task: 'WortheyFlow health monitoring', type: 'background', lastSeen: new Date().toISOString() },
  { name: 'MilePilot Product Agent', status: 'active', task: 'Auto-detection + swipe UX', type: 'active', lastSeen: new Date().toISOString() },
  { name: 'Platform Agent', status: 'active', task: 'Mission Control + gateway', type: 'active', lastSeen: new Date().toISOString() },
];

// Track server uptime
const startTime = Date.now();

// Initialize data directory and load persisted data
async function initDataStore() {
  try {
    await fs.mkdir(DATA_DIR, { recursive: true });

    // Load tasks
    try {
      const tasksData = await fs.readFile(TASKS_FILE, 'utf8');
      tasks = JSON.parse(tasksData);
      console.log(`📋 Loaded ${tasks.length} tasks`);
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
      tasks = [];
    }

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

    // Load agent registry
    try {
      const agentsData = await fs.readFile(AGENTS_FILE, 'utf8');
      const loadedAgents = JSON.parse(agentsData);
      if (loadedAgents.length > 0) {
        agentRegistry = loadedAgents;
        console.log(`🤖 Loaded ${agentRegistry.length} agents`);
      }
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
      // Keep default agents
    }
  } catch (err) {
    console.error('Failed to initialize data store:', err);
  }
}

// Persist data to disk
async function persistData() {
  try {
    await fs.writeFile(TASKS_FILE, JSON.stringify(tasks, null, 2));
    await fs.writeFile(ACTIVITY_FILE, JSON.stringify(activityLog, null, 2));
    await fs.writeFile(CREATORS_FILE, JSON.stringify(creators, null, 2));
    await fs.writeFile(AGENTS_FILE, JSON.stringify(agentRegistry, null, 2));
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
function runCreatorFollowUp() {
  const now = new Date();
  let changesCount = 0;

  creators.forEach(creator => {
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

      tasks.unshift(task);
      creator.taskCreated = true;
      creator.taskId = task.id;
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

      tasks.unshift(task);
      creator.onboardingTaskCreated = true;
      creator.onboardingTaskId = task.id;
      creator.updatedAt = nowISO;
      changesCount++;
    }

    // Check if onboarding task completed → move to active
    if (creator.stage === 'committed' && creator.onboardingTaskId) {
      const onboardingTask = tasks.find(t => t.id === creator.onboardingTaskId);
      if (onboardingTask && onboardingTask.status === 'completed') {
        creator.stage = 'active';
        creator.nextAction = 'Creator is active';
        creator.updatedAt = now.toISOString();
        changesCount++;
      }
    }
  });

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
function runSlaEnforcement() {
  const now = new Date().toISOString();
  let changesCount = 0;

  tasks.forEach(task => {
    // Only enforce on active tasks
    if (!['pending', 'in_progress', 'assigned', 'blocked'].includes(task.status)) {
      return;
    }

    // Skip if no SLA deadline
    if (!task.slaDeadline) {
      return;
    }

    // Calculate current SLA status
    const newStatus = calculateSlaStatus(task.slaDeadline, task.createdAt, task.sla);
    const oldStatus = task.slaStatus;

    task.slaStatus = newStatus;
    task.timeLeft = formatTimeLeft(task.slaDeadline, newStatus);

    // Auto-block if overdue (but not already blocked)
    if (newStatus === 'overdue' && !task.slaOverdueAt) {
      task.slaOverdueAt = now;

      // Auto-change to blocked if pending/in_progress/assigned
      if (['pending', 'in_progress', 'assigned'].includes(task.status)) {
        task.status = 'blocked';
        task.detail = task.detail ? `${task.detail}\n\n[SLA OVERDUE — auto-blocked]` : '[SLA OVERDUE — auto-blocked]';
        changesCount++;
      }
    }

    // Escalate if past 2x duration
    if (newStatus === 'escalated' && !task.slaEscalatedAt) {
      task.slaEscalatedAt = now;

      // If Bot-owned, mark as botBlocked
      if (task.owner === 'Bot') {
        task.botBlocked = true;
        task.title = task.title.startsWith('⚠️') ? task.title : `⚠️ BOT BLOCKED: ${task.title}`;
        changesCount++;
      }
    }
  });

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

    tasks.unshift(task);

    // Keep only last 1000 tasks
    if (tasks.length > 1000) {
      tasks = tasks.slice(0, 1000);
    }

    await persistData();
    res.json(task);
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
    const agentInProgress = tasks.find(t =>
      t.agent === agentName &&
      t.status === 'in_progress'
    );

    if (agentInProgress) {
      return res.status(409).json({
        error: 'Agent already has a task in progress',
        currentTask: agentInProgress
      });
    }

    // Find first queued task matching company (if specified)
    const queuedTask = tasks.find(t =>
      t.status === 'queued' &&
      (!company || t.company === company)
    );

    if (!queuedTask) {
      return res.json({ task: null, message: 'Queue empty' });
    }

    // Claim the task
    const now = new Date().toISOString();
    queuedTask.status = 'in_progress';
    queuedTask.agent = agentName;
    queuedTask.startedAt = now;
    queuedTask.lastActionAt = now;
    queuedTask.updatedAt = now;

    await persistData();
    res.json({ task: queuedTask });
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
    const existingIdx = agentRegistry.findIndex(a => a.name === name);

    const agent = {
      id: existingIdx >= 0 ? agentRegistry[existingIdx].id : generateId(),
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
      createdAt: existingIdx >= 0 ? agentRegistry[existingIdx].createdAt : now,
      updatedAt: now
    };

    if (existingIdx >= 0) {
      agentRegistry[existingIdx] = agent;
    } else {
      agentRegistry.push(agent);
    }

    await persistData();
    res.json(agent);
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

    const agent = agentRegistry.find(a => a.name === name);
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }

    const now = new Date().toISOString();

    agent.lastHeartbeat = now;
    agent.updatedAt = now;
    agent.lastAction = lastAction || agent.lastAction;
    agent.error = error || null;

    if (currentTaskId) {
      agent.currentTaskId = currentTaskId;
      agent.status = 'working';

      // Find the task
      const task = tasks.find(t => t.id === currentTaskId);
      if (task) {
        agent.currentTask = {
          id: task.id,
          title: task.title,
          company: task.company,
          status: task.status
        };
      }
    } else {
      agent.currentTaskId = null;
      agent.currentTask = null;
      agent.status = 'idle';
    }

    await persistData();
    res.json({ ok: true });
  } catch (err) {
    console.error('Agent heartbeat error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/agents/health — Agent health summary
app.get('/api/agents/health', (req, res) => {
  try {
    const now = Date.now();

    const healthSummary = agentRegistry.map(agent => {
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

    const task = tasks.find(t => t.id === id);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const now = new Date().toISOString();
    task.status = 'in_progress';
    task.startedAt = now;
    task.agent = agent || task.agent;
    task.currentStep = currentStep || null;
    task.lastActionAt = now;
    task.updatedAt = now;

    await persistData();
    res.json({ success: true, task });
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

    const task = tasks.find(t => t.id === id);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const now = new Date().toISOString();
    task.status = 'completed';
    task.completedAt = now;
    task.lastActionAt = now;
    task.updatedAt = now;

    if (detail) {
      task.detail = task.detail ? `${task.detail}\n\n[COMPLETED] ${detail}` : detail;
    }

    await persistData();
    res.json({ success: true, task });
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

    const task = tasks.find(t => t.id === id);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const now = new Date().toISOString();
    task.status = blocked ? 'blocked' : 'failed';
    task.error = error || 'Task failed';
    task.lastActionAt = now;
    task.updatedAt = now;

    if (error) {
      task.detail = task.detail ? `${task.detail}\n\n[ERROR] ${error}` : `[ERROR] ${error}`;
    }

    await persistData();
    res.json({ success: true, task });
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

    const task = tasks.find(t => t.id === id);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const now = new Date().toISOString();

    if (currentStep !== undefined) {
      task.currentStep = currentStep;
    }

    if (nextAction !== undefined) {
      task.nextAction = nextAction;
    }

    task.lastActionAt = now;
    task.updatedAt = now;

    await persistData();
    res.json({ success: true, task });
  } catch (err) {
    console.error('Task step update error:', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/agents/:id — Delete agent
app.delete('/api/agents/:id', async (req, res) => {
  try {
    const { id } = req.params;

    // Try to find by id or name
    const agentIdx = agentRegistry.findIndex(a => a.id === id || a.name === id);

    if (agentIdx === -1) {
      return res.status(404).json({ error: 'Agent not found' });
    }

    const deleted = agentRegistry.splice(agentIdx, 1)[0];
    await persistData();
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
    const taskIdx = tasks.findIndex(t => t.id === id);

    if (taskIdx === -1) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const deleted = tasks.splice(taskIdx, 1)[0];
    await persistData();
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
    const existing = tasks.find(t =>
      t.title === taskTitle &&
      t.createdAt > oneHourAgo
    );

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

    tasks.unshift(task);

    if (tasks.length > 1000) {
      tasks = tasks.slice(0, 1000);
    }

    await persistData();
    res.json({ created: true, task });
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
    const existing = tasks.find(t =>
      t.title === taskTitle &&
      t.createdAt > oneHourAgo
    );

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

    tasks.unshift(task);

    if (tasks.length > 1000) {
      tasks = tasks.slice(0, 1000);
    }

    await persistData();
    res.json({ created: true, task });
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

    tasks.unshift(task);

    // Keep only last 1000 tasks
    if (tasks.length > 1000) {
      tasks = tasks.slice(0, 1000);
    }

    await persistData();
    res.json({ success: true, task });
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

    const task = tasks.find(t => t.id === id);
    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    const now = new Date().toISOString();

    // Track if status changed
    const statusChanged = updates.status && updates.status !== task.status;
    const slaChanged = updates.sla && updates.sla !== task.sla;

    // Apply updates
    Object.keys(updates).forEach(key => {
      if (key !== 'id' && key !== 'createdAt') {
        task[key] = updates[key];
      }
    });

    task.updatedAt = now;

    // Auto-set lastActionAt when status changes
    if (statusChanged) {
      task.lastActionAt = now;
    }

    // Set completedAt if status changed to completed
    if (updates.status === 'completed' && !task.completedAt) {
      task.completedAt = now;
    }

    // Recalculate tier if estimatedValue changed
    if (updates.estimatedValue) {
      task.tier = calculateTier(updates.estimatedValue);
    }

    // Recalculate SLA deadline if sla changed
    if (slaChanged) {
      task.slaDeadline = calculateSlaDeadline(task.sla, task.createdAt);
      task.slaStatus = calculateSlaStatus(task.slaDeadline, task.createdAt, task.sla);
      task.timeLeft = formatTimeLeft(task.slaDeadline, task.slaStatus);
    }

    await persistData();
    res.json({ success: true, task });
  } catch (err) {
    console.error('Task update error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/tasks - List tasks with filters
app.get('/api/tasks', (req, res) => {
  try {
    let filtered = tasks.filter(t => t.status !== 'archived');

    // Apply filters
    if (req.query.company) {
      filtered = filtered.filter(t => t.company === req.query.company);
    }
    if (req.query.category) {
      filtered = filtered.filter(t => t.category === req.query.category);
    }
    if (req.query.status) {
      filtered = filtered.filter(t => t.status === req.query.status);
    }
    if (req.query.owner) {
      filtered = filtered.filter(t => t.owner === req.query.owner);
    }

    res.json(filtered);
  } catch (err) {
    console.error('Task list error:', err);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/task/:id - Soft delete (archive)
app.delete('/api/task/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const task = tasks.find(t => t.id === id);

    if (!task) {
      return res.status(404).json({ error: 'Task not found' });
    }

    task.status = 'archived';
    task.updatedAt = new Date().toISOString();

    await persistData();
    res.json({ success: true, task });
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

    tasks.forEach(task => {
      // Only roll forward incomplete tasks with TODAY or NOW SLA
      if (['pending', 'in_progress', 'assigned'].includes(task.status) &&
          (task.sla === 'TODAY' || task.sla === 'NOW')) {

        // Mark as rolled forward
        task.rolledForward = true;

        // Boost priority
        if (!task.originalPriority) {
          task.originalPriority = task.tier;
        }

        if (task.tier === 'LOW') {
          task.tier = 'MEDIUM';
        } else if (task.tier === 'MEDIUM') {
          task.tier = 'HIGH';
        }

        // Reset SLA to TODAY for tomorrow
        task.sla = 'TODAY';
        task.slaDeadline = tomorrowEod;
        task.slaStatus = 'on_track';
        task.timeLeft = formatTimeLeft(task.slaDeadline, 'on_track');

        // Reset overdue timestamps
        task.slaOverdueAt = null;
        task.slaEscalatedAt = null;

        // Add note
        const rollNote = `[Rolled forward from ${now.toLocaleDateString('en-US', { timeZone: 'America/Chicago' })}]`;
        task.detail = task.detail ? `${task.detail}\n${rollNote}` : rollNote;

        rolledCount++;
      }
    });

    await persistData();
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
app.get('/api/creator-pipeline', (req, res) => {
  try {
    // Run follow-up automation
    runCreatorFollowUp();

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
    runSlaEnforcement();

    const [oaData, waData, mpData, smsData, commsData] = await Promise.all([
      fetchOAData(),
      fetchWAData(),
      fetchMPData(),
      fetch('https://disciplined-alignment-production.up.railway.app/api/sms-status')
        .then(r => r.json()).catch(() => ({ status: 'UNKNOWN', successRate: 'N/A' })),
      fetch('https://disciplined-alignment-production.up.railway.app/api/comms-status')
        .then(r => r.json()).catch(() => ({ rates: {}, stats: {}, noReply24h: 0 }))
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

      tasks.unshift(task);
      if (tasks.length > 1000) {
        tasks = tasks.slice(0, 1000);
      }
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

    tasks.unshift(task);
    if (tasks.length > 1000) {
      tasks = tasks.slice(0, 1000);
    }

    await persistData();
    res.json({ success: true, approval: task });
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

    const task = tasks.find(t => t.id === id);
    if (!task) {
      return res.status(404).json({ error: 'Approval not found' });
    }

    const now = new Date().toISOString();

    if (action === 'approve') {
      task.status = 'completed';
      task.completedAt = now;
      task.lastActionAt = now;
    } else if (action === 'reject') {
      task.status = 'archived';
      task.detail = `${task.detail}\n[REJECTED: ${reason || 'No reason provided'}]`.trim();
      task.lastActionAt = now;
    }

    task.updatedAt = now;

    await persistData();
    res.json({ success: true, approval: task });
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

    tasks.unshift(task);
    if (tasks.length > 1000) {
      tasks = tasks.slice(0, 1000);
    }

    await persistData();
    res.json({ success: true, blocker: task });
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
      tasks.length = 0;
      activityLog.length = 0;
      creators.length = 0;
      await persistData();
      return res.json({ success: true, message: 'All data cleared' });
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
app.get('/api/agents', (req, res) => {
  // Agent registry — updated via POST /api/agent-heartbeat or manually
  res.json(agentRegistry || []);
});

// POST /api/agent-heartbeat — Agents report their status
app.post('/api/agent-heartbeat', (req, res) => {
  const { name, status, task, type, lastSeen } = req.body;
  if (!name) return res.status(400).json({ error: 'name required' });
  
  const idx = agentRegistry.findIndex(a => a.name === name);
  const agent = {
    name,
    status: status || 'active',
    task: task || '',
    type: type || 'background',
    lastSeen: lastSeen || new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
  
  if (idx >= 0) {
    agentRegistry[idx] = { ...agentRegistry[idx], ...agent };
  } else {
    agentRegistry.push(agent);
  }
  
  res.json({ ok: true, agent: agentRegistry[idx >= 0 ? idx : agentRegistry.length - 1] });
});

app.get('/api/health', (req, res) => {
  const now = Date.now();
  const uptimeSec = Math.floor((now - startTime) / 1000);

  // Calculate agent stats
  const agentStats = {
    total: agentRegistry.length,
    working: agentRegistry.filter(a => a.status === 'working').length,
    idle: agentRegistry.filter(a => a.status === 'idle').length,
    stale: agentRegistry.filter(a => {
      const lastHeartbeat = a.lastHeartbeat ? new Date(a.lastHeartbeat).getTime() : 0;
      const staleSec = Math.floor((now - lastHeartbeat) / 1000);
      return staleSec > 120;
    }).length
  };

  // Calculate task stats
  const taskStats = {
    total: tasks.filter(t => t.status !== 'archived').length,
    queued: tasks.filter(t => t.status === 'queued').length,
    inProgress: tasks.filter(t => t.status === 'in_progress').length,
    completed: tasks.filter(t => t.status === 'completed').length
  };

  res.json({
    ok: true,
    uptimeSec,
    agents: agentStats,
    tasks: taskStats
  });
});

app.get('/health', (req, res) => {
  const now = Date.now();
  const uptimeSec = Math.floor((now - startTime) / 1000);

  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptimeSec,
    taskCount: tasks.filter(t => t.status !== 'archived').length,
    needsApproval: tasks.filter(t => t.status === 'needs_approval').length,
    blocked: tasks.filter(t => t.status === 'blocked').length,
    overdue: tasks.filter(t => t.slaStatus === 'overdue' || t.slaStatus === 'escalated').length,
    agentCount: agentRegistry.length
  });
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
initDataStore().then(() => {
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Mission Control V3 (Daily Operating System) running on port ${PORT}`);
    console.log(`📋 Tasks: ${tasks.filter(t => t.status !== 'archived').length}`);
    console.log(`🔴 Needs Approval: ${tasks.filter(t => t.status === 'needs_approval').length}`);
    console.log(`🚨 Blockers: ${tasks.filter(t => t.status === 'blocked').length}`);
  });
});
