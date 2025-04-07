import React from "react";

const Trip = () => {
    return (
        <div style={styles.container}>
            <header style={styles.header}>
                <h1 style={styles.title}>Grab Trip</h1>
            </header>
            <main style={styles.main}>
                <section style={styles.tripDetails}>
                    <h2 style={styles.sectionTitle}>Trip Details</h2>
                    <p>Pickup: 123 Main Street</p>
                    <p>Destination: 456 Elm Street</p>
                    <p>Fare: $15.00</p>
                </section>
                <section style={styles.driverDetails}>
                    <h2 style={styles.sectionTitle}>Driver Details</h2>
                    <p>Name: John Doe</p>
                    <p>Car: Toyota Prius</p>
                    <p>License Plate: ABC-1234</p>
                </section>
                <button style={styles.button}>Contact Driver</button>
            </main>
        </div>
    );
};

const styles = {
    container: {
        fontFamily: "Arial, sans-serif",
        maxWidth: "400px",
        margin: "0 auto",
        padding: "20px",
        backgroundColor: "#f9f9f9",
        borderRadius: "8px",
        boxShadow: "0 4px 6px rgba(0, 0, 0, 0.1)",
    },
    header: {
        textAlign: "center",
        marginBottom: "20px",
    },
    title: {
        fontSize: "24px",
        color: "#4CAF50",
    },
    main: {
        display: "flex",
        flexDirection: "column",
        gap: "20px",
    },
    tripDetails: {
        backgroundColor: "#fff",
        padding: "15px",
        borderRadius: "8px",
        boxShadow: "0 2px 4px rgba(0, 0, 0, 0.1)",
    },
    driverDetails: {
        backgroundColor: "#fff",
        padding: "15px",
        borderRadius: "8px",
        boxShadow: "0 2px 4px rgba(0, 0, 0, 0.1)",
    },
    sectionTitle: {
        fontSize: "18px",
        marginBottom: "10px",
        color: "#333",
    },
    button: {
        padding: "10px 20px",
        fontSize: "16px",
        color: "#fff",
        backgroundColor: "#4CAF50",
        border: "none",
        borderRadius: "8px",
        cursor: "pointer",
        textAlign: "center",
    },
};

export default Trip;