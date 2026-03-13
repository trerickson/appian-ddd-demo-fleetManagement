-- phpMyAdmin SQL Dump
-- version 5.2.3
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Mar 12, 2026 at 07:09 PM
-- Server version: 10.11.13-MariaDB-log
-- PHP Version: 8.3.0

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `Appian`
--

-- --------------------------------------------------------

--
-- Table structure for table `TDFD_MAINTENANCE_TYPE`
--

CREATE TABLE IF NOT EXISTS `TDFD_MAINTENANCE_TYPE` (
  `MAINTENANCE_TYPE_ID` int(11) NOT NULL,
  `VALUE` varchar(255) DEFAULT NULL,
  `DESCRIPTION` varchar(4000) DEFAULT NULL,
  `IS_ACTIVE` bit(1) DEFAULT NULL,
  `LAST_UPDATED_ON` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`MAINTENANCE_TYPE_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `TDFD_MAINTENANCE_TYPE`
--

INSERT INTO `TDFD_MAINTENANCE_TYPE` (`MAINTENANCE_TYPE_ID`, `VALUE`, `DESCRIPTION`, `IS_ACTIVE`, `LAST_UPDATED_ON`) VALUES
(1, 'Standard Service', 'Rountine Maintenance i.e. Tire Rotation, Oil Change', b'1', '2026-02-10 13:00:00.000000'),
(2, 'Initial Inspection', 'First inspection completed when a new vehicle enters the lot', b'1', '2026-02-10 13:00:00.000000'),
(3, 'Repair', 'Unscheduled Repair to a vehicle', b'1', '2026-02-10 13:00:00.000000');

-- --------------------------------------------------------

--
-- Table structure for table `TDFD_STATUS`
--

CREATE TABLE IF NOT EXISTS `TDFD_STATUS` (
  `STATUS_ID` int(11) NOT NULL,
  `VALUE` varchar(255) DEFAULT NULL,
  `DESCRIPTION` varchar(4000) DEFAULT NULL,
  `IS_ACTIVE` varchar(255) DEFAULT NULL,
  `LAST_UPDATED_ON` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`STATUS_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `TDFD_STATUS`
--

INSERT INTO `TDFD_STATUS` (`STATUS_ID`, `VALUE`, `DESCRIPTION`, `IS_ACTIVE`, `LAST_UPDATED_ON`) VALUES
(1, 'In Progress', 'The Maintenance has begun and the vehicle was removed from the fleet', '1', '2026-02-10 13:00:00.000000'),
(2, 'Waiting for Parts', 'The maintenance order required parts to complete. The work cannot be completed until the parts are delivered and installed', '1', '2026-02-10 13:00:00.000000'),
(3, 'Completed', 'The maintenance is complete and the vehicle is returned to the fleet', '1', '2026-02-10 13:00:00.000000');
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
