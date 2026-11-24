import React, { useEffect, useState } from "react";

const API = import.meta.env.VITE_API_URL || "http://localhost:8081";
async function request(path, options) {
  const r = await fetch(`${API}${path}`, { headers: { "Content-Type": "application/json" }, ...options });
  const data = await r.json();
  if (!r.ok) throw new Error(data.error || "Request failed");
  return data;
}
export default function App() {
  const [activities,setActivities] = useState([]);
  const [events,setEvents] = useState({processed:[],failed:[]});
  const [stats,setStats] = useState({activities:0,processed:0,failed:0});
  const [busy,setBusy] = useState(false);
  const [notice,setNotice] = useState("Connecting to EventPulse...");
  const [lastEvent,setLastEvent] = useState(null);

  async function refresh() {
    try {
      const [a,e,s] = await Promise.all([request("/api/activities"),request("/api/events"),request("/api/stats")]);
      setActivities(a); setEvents(e); setStats(s); setNotice("Connected — live data loaded from Firestore");
    } catch (err) { setNotice(`Connection issue: ${err.message}`); }
  }
  useEffect(() => { refresh(); const t=setInterval(refresh,2500); return ()=>clearInterval(t); }, []);

  async function action(path, body={}) {
    setBusy(true);
    try {
      const result = await request(path,{method:"POST",body:JSON.stringify(body)});
      if (result.event) setLastEvent(result.event);
      setNotice(result.message || "Request completed");
      setTimeout(refresh,500);
    } catch(err) { setNotice(`Error: ${err.message}`); }
    finally { setBusy(false); }
  }
  const sendSample = () => action("/api/events",{
    event_type:"assignment_submitted",student_id:"STU101",activity_id:"ASSIGN101",
    data:{status:"submitted"}
  });
  const replay = () => lastEvent
    ? action("/api/events",lastEvent)
    : setNotice("Send a sample event first, then replay it.");
  return <main className="shell">
    <header className="top">
      <div><div className="eyebrow">CLOUD EVENT PROCESSING DEMO</div><h1>Event<span>Pulse</span></h1>
      <p>Student activity events, processed reliably.</p></div>
      <div className="live"><i/> LOCAL EMULATOR MODE</div>
    </header>
    <div className="notice">{notice}</div>
    <section className="stats">
      <article><small>STUDENT ACTIVITIES</small><strong>{stats.activities}</strong><span>Firestore records</span></article>
      <article><small>PROCESSED EVENTS</small><strong>{stats.processed}</strong><span>Unique event IDs</span></article>
      <article><small>FAILED / REJECTED</small><strong>{stats.failed}</strong><span>Quarantined records</span></article>
    </section>
    <section className="panel">
      <div className="section-head"><div><div className="eyebrow">PRODUCER CONTROLS</div><h2>Send a test event</h2></div><span className="tag">Synthetic data only</span></div>
      <p className="muted">Publish sample events and observe how the backend validates, processes and stores them.</p>
      <div className="actions">
        <button disabled={busy} onClick={sendSample}>＋ Send sample event</button>
        <button className="secondary" disabled={busy} onClick={replay}>↻ Replay last event</button>
        <button className="secondary" disabled={busy} onClick={()=>action("/api/test/malformed")}>Send malformed event</button>
        <button className="warning" disabled={busy} onClick={()=>action("/api/test/temporary-failure")}>Simulate temporary failure</button>
      </div>
      {lastEvent && <details><summary>Last published event payload</summary><pre>{JSON.stringify(lastEvent,null,2)}</pre></details>}
    </section>
    <section className="panel">
      <div className="section-head"><div><div className="eyebrow">DATABASE</div><h2>Student activities</h2></div><button className="small secondary" onClick={refresh}>Refresh</button></div>
      {activities.length===0 ? <p className="empty">No activity records yet. Send a sample event to get started.</p> :
      <div className="table-wrap"><table><thead><tr><th>Student</th><th>Activity</th><th>Status</th><th>Last event ID</th></tr></thead>
      <tbody>{activities.map(a=><tr key={a.id}><td>{a.student_id}</td><td>{a.activity_id}</td><td><span className="pill">{a.status}</span></td><td className="mono">{a.last_event_id}</td></tr>)}</tbody></table></div>}
    </section>
    <section className="two">
      <section className="panel"><div className="eyebrow">EVENT TRACE</div><h2>Processed events</h2>
        {events.processed.length===0?<p className="empty">No processed events yet.</p>:events.processed.map(e=><div className="event" key={e.id}><b>{e.event_id}</b><span className="success">PROCESSED</span><small>{e.student_id} / {e.activity_id}</small></div>)}
      </section>
      <section className="panel"><div className="eyebrow">ERROR HANDLING</div><h2>Failed / rejected</h2>
        {events.failed.length===0?<p className="empty">No failures or rejected events.</p>:events.failed.map(e=><div className="event" key={e.id}><b>{e.event_id || e.id}</b><span className="failure">{e.status}</span><small>{e.reason}</small></div>)}
      </section>
    </section>
    <footer>EventPulse · React + Express + Pub/Sub emulator + Firestore emulator · Use synthetic records only</footer>
  </main>;
}
