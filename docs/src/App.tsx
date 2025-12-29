import { Hero } from './sections/01-Hero';
import { ThePitch } from './sections/02-ThePitch';
import { QuickStart } from './sections/03-QuickStart';
import { FarmDashboard } from './sections/03-FarmDashboard';
import { DeepDive } from './sections/04-DeepDive';
import { APIReference } from './sections/05-APIReference';
import { Navigation } from './components/Navigation';
import { Footer } from './components/Footer';

function App() {
  return (
    <div className="min-h-screen bg-[#0a0a0a]">
      <Navigation />
      
      <main>
        {/* 1. Hook them */}
        <Hero />
        
        {/* 2. Explain why this matters */}
        <ThePitch />
        
        {/* 3. Show how easy it is */}
        <QuickStart />
        
        {/* 4. Impress with a real demo */}
        <FarmDashboard />
        
        {/* 5. Explain how it works */}
        <DeepDive />
        
        {/* 6. Reference docs */}
        <APIReference />
      </main>
      
      <Footer />
    </div>
  );
}

export default App;
